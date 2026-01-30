import Redis from 'ioredis';
import { logger } from '../utils/logger';

/**
 * Redis Connection Manager
 * 
 * Provides Redis connectivity for:
 * 1. BullMQ job queues (sheet-to-db, db-to-sheet)
 * 2. Distributed locks for loop prevention
 * 3. Rate limiting counters
 * 4. Caching (optional)
 */

interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db: number;
  maxRetriesPerRequest: null; // Required for BullMQ
  enableReadyCheck: boolean;
  retryStrategy: (times: number) => number | void;
  tls?: {}; // TLS configuration for secure connections
  family: 4; // Force IPv4
}

class RedisManager {
  private client: Redis | null = null;
  private subscriber: Redis | null = null;
  private config: RedisConfig;
  private isConnected: boolean = false;

  constructor() {
    this.config = {
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379', 10),
      password: process.env.REDIS_PASSWORD || undefined,
      db: parseInt(process.env.REDIS_DB || '0', 10),
      maxRetriesPerRequest: null, // BullMQ requirement
      enableReadyCheck: true,
      family: 4, // Force IPv4
      tls: {}, // Enable TLS for Aiven Valkey
      retryStrategy: (times: number) => {
        // Exponential backoff with max 30 seconds
        const delay = Math.min(times * 1000, 30000);
        logger.warn(`Redis connection retry attempt ${times}, waiting ${delay}ms`);
        return delay;
      },
    };
  }

  /**
   * Initialize Redis connections
   * Creates separate connections for commands and pub/sub
   */
  async initialize(): Promise<void> {
    if (this.client) {
      logger.warn('Redis client already initialized');
      return;
    }

    try {
      // Main client for commands
      this.client = new Redis(this.config);
      
      // Separate client for BullMQ subscriber (required for event listening)
      this.subscriber = new Redis(this.config);

      // Set up event handlers
      this.setupEventHandlers(this.client, 'main');
      this.setupEventHandlers(this.subscriber, 'subscriber');

      // Wait for connection
      await this.waitForConnection();

      logger.info('Redis connections initialized successfully', {
        host: this.config.host,
        port: this.config.port,
        db: this.config.db,
      });
    } catch (error) {
      logger.error('Failed to initialize Redis connections', { error });
      throw error;
    }
  }

  /**
   * Set up event handlers for Redis client
   */
  private setupEventHandlers(client: Redis, name: string): void {
    client.on('connect', () => {
      logger.debug(`Redis ${name} client connecting...`);
    });

    client.on('ready', () => {
      this.isConnected = true;
      logger.info(`Redis ${name} client ready`);
    });

    client.on('error', (error) => {
      logger.error(`Redis ${name} client error`, { error: error.message });
    });

    client.on('close', () => {
      this.isConnected = false;
      logger.warn(`Redis ${name} client connection closed`);
    });

    client.on('reconnecting', () => {
      logger.info(`Redis ${name} client reconnecting...`);
    });
  }

  /**
   * Wait for Redis connection to be ready
   */
  private async waitForConnection(timeout: number = 10000): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.client?.status === 'ready') {
        resolve();
        return;
      }

      const timer = setTimeout(() => {
        reject(new Error(`Redis connection timeout after ${timeout}ms`));
      }, timeout);

      this.client?.once('ready', () => {
        clearTimeout(timer);
        resolve();
      });

      this.client?.once('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });
    });
  }

  /**
   * Get the main Redis client for commands
   * Used by BullMQ and general Redis operations
   */
  getClient(): Redis {
    if (!this.client) {
      throw new Error('Redis client not initialized. Call initialize() first.');
    }
    return this.client;
  }

  /**
   * Get the subscriber client
   * Used by BullMQ for event subscriptions
   */
  getSubscriber(): Redis {
    if (!this.subscriber) {
      throw new Error('Redis subscriber not initialized. Call initialize() first.');
    }
    return this.subscriber;
  }

  /**
   * Create a new Redis connection for BullMQ
   * BullMQ requires creating new connections for each Queue/Worker
   */
  createConnection(): Redis {
    return new Redis(this.config);
  }

  /**
   * Acquire a distributed lock
   * Uses Redis SET NX EX pattern for atomic lock acquisition
   * 
   * @param key - Lock key
   * @param ttlMs - Lock TTL in milliseconds
   * @param value - Lock value (for ownership verification)
   * @returns true if lock acquired, false otherwise
   */
  async acquireLock(key: string, ttlMs: number, value: string): Promise<boolean> {
    const client = this.getClient();
    const result = await client.set(key, value, 'PX', ttlMs, 'NX');
    return result === 'OK';
  }

  /**
   * Release a distributed lock
   * Only releases if the lock value matches (prevents releasing others' locks)
   */
  async releaseLock(key: string, value: string): Promise<boolean> {
    const client = this.getClient();
    
    // Lua script for atomic check-and-delete
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    
    const result = await client.eval(script, 1, key, value);
    return result === 1;
  }

  /**
   * Increment a rate limit counter
   * Used for tracking API call rates
   * 
   * @param key - Rate limit key (e.g., "ratelimit:sheets:2024-01-15T10:30")
   * @param windowMs - Window duration in milliseconds
   * @returns Current count after increment
   */
  async incrementRateLimit(key: string, windowMs: number): Promise<number> {
    const client = this.getClient();
    const multi = client.multi();
    
    multi.incr(key);
    multi.pexpire(key, windowMs);
    
    const results = await multi.exec();
    return results?.[0]?.[1] as number || 0;
  }

  /**
   * Get current rate limit count
   */
  async getRateLimitCount(key: string): Promise<number> {
    const client = this.getClient();
    const count = await client.get(key);
    return parseInt(count || '0', 10);
  }

  /**
   * Test Redis connectivity
   */
  async testConnection(): Promise<boolean> {
    try {
      const client = this.getClient();
      const pong = await client.ping();
      return pong === 'PONG';
    } catch (error) {
      logger.error('Redis health check failed', { error });
      return false;
    }
  }

  /**
   * Check if Redis is connected
   */
  isHealthy(): boolean {
    return this.isConnected && this.client !== null;
  }

  /**
   * Get Redis info/stats
   */
  async getStats(): Promise<Record<string, string>> {
    const client = this.getClient();
    const info = await client.info();
    
    const stats: Record<string, string> = {};
    info.split('\n').forEach((line) => {
      const [key, value] = line.split(':');
      if (key && value) {
        stats[key.trim()] = value.trim();
      }
    });
    
    return stats;
  }

  /**
   * Gracefully close Redis connections
   */
  async close(): Promise<void> {
    const closePromises: Promise<void>[] = [];

    if (this.client) {
      closePromises.push(
        this.client.quit().then(() => {
          this.client = null;
        })
      );
    }

    if (this.subscriber) {
      closePromises.push(
        this.subscriber.quit().then(() => {
          this.subscriber = null;
        })
      );
    }

    await Promise.all(closePromises);
    this.isConnected = false;
    logger.info('Redis connections closed');
  }
}

// Export a singleton instance
export const redis = new RedisManager();

// Also export the class for testing purposes
export { RedisManager };
