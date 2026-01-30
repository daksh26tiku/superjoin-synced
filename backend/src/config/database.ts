import mysql, { Pool, PoolConnection, ResultSetHeader, RowDataPacket, OkPacket } from 'mysql2/promise';
import { logger } from '../utils/logger';

/**
 * MySQL Database Connection Manager
 * 
 * Implements a connection pool pattern for efficient database access.
 * The pool automatically handles connection lifecycle, reconnection,
 * and connection limits to prevent exhausting database resources.
 */

interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  connectionLimit: number;
  queueLimit: number;
  waitForConnections: boolean;
  enableKeepAlive: boolean;
  keepAliveInitialDelay: number;
  ssl?: {}; // SSL configuration for Aiven MySQL
}

class DatabaseManager {
  private pool: Pool | null = null;
  private config: DatabaseConfig;
  private isConnected: boolean = false;

  constructor() {
    this.config = {
      host: process.env.MYSQL_HOST || 'localhost',
      port: parseInt(process.env.MYSQL_PORT || '3307', 10),
      user: process.env.MYSQL_USER || 'sync_user',
      password: process.env.MYSQL_PASSWORD || 'sync_password',
      database: process.env.MYSQL_DATABASE || 'sheets_sync',
      connectionLimit: parseInt(process.env.MYSQL_CONNECTION_LIMIT || '10', 10),
      queueLimit: parseInt(process.env.MYSQL_QUEUE_LIMIT || '0', 10),
      waitForConnections: true,
      enableKeepAlive: true,
      keepAliveInitialDelay: 10000,
      ssl: {}, // Enable SSL for Aiven MySQL
    };
  }

  /**
   * Initialize the connection pool
   * Should be called once at application startup
   */
  async initialize(): Promise<void> {
    if (this.pool) {
      logger.warn('Database pool already initialized');
      return;
    }

    try {
      logger.info('MySQL configuration loaded', {
        host: this.config.host,
        port: this.config.port,
        database: this.config.database,
        connectionLimit: this.config.connectionLimit,
      });

      this.pool = mysql.createPool({
        ...this.config,
        namedPlaceholders: true,
        dateStrings: true,
        timezone: 'Z', // UTC timezone for consistency
      });

      // Test the connection
      await this.testConnection();
      
      this.isConnected = true;
      logger.info('MySQL connection pool initialized successfully', {
        host: this.config.host,
        port: this.config.port,
        database: this.config.database,
        connectionLimit: this.config.connectionLimit,
      });
    } catch (error) {
      logger.error('Failed to initialize MySQL connection pool', { error });
      throw error;
    }
  }

  /**
   * Test database connectivity
   * Executes a simple query to verify the connection is working
   */
  async testConnection(): Promise<boolean> {
    try {
      const connection = await this.getConnection();
      try {
        await connection.query('SELECT 1 as health_check');
        logger.debug('Database health check passed');
        return true;
      } finally {
        connection.release();
      }
    } catch (error) {
      logger.error('Database health check failed', { error });
      this.isConnected = false;
      return false;
    }
  }

  /**
   * Get a connection from the pool
   * Remember to release the connection after use!
   */
  async getConnection(): Promise<PoolConnection> {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call initialize() first.');
    }
    const connection = await this.pool.getConnection();
    return connection;
  }

  /**
   * Execute a query using the pool directly
   * Automatically handles connection acquisition and release
   * 
   * @param sql - SQL query string (can use named placeholders with :paramName)
   * @param params - Query parameters
   */
  async query<T = unknown>(
    sql: string,
    params?: Record<string, unknown> | unknown[]
  ): Promise<T> {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call initialize() first.');
    }

    try {
      const [rows] = await this.pool.query(sql, params);
      return rows as T;
    } catch (error) {
      logger.error('Query execution failed', { sql, error });
      throw error;
    }
  }

  /**
   * Execute an INSERT, UPDATE, or DELETE query
   * Returns the result set header with affectedRows, insertId, etc.
   */
  async execute(
    sql: string,
    params?: Record<string, unknown> | unknown[]
  ): Promise<OkPacket> {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call initialize() first.');
    }

    try {
      const [result] = await this.pool.execute(sql, params);
      return result as OkPacket;
    } catch (error) {
      logger.error('Execute failed', { sql, error });
      throw error;
    }
  }

  /**
   * Execute multiple queries within a transaction
   * Automatically handles commit/rollback
   */
  async transaction<T>(
    callback: (connection: PoolConnection) => Promise<T>
  ): Promise<T> {
    const connection = await this.getConnection();
    
    try {
      await connection.beginTransaction();
      const result = await callback(connection);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      logger.error('Transaction rolled back', { error });
      throw error;
    } finally {
      connection.release();
    }
  }

  /**
   * Execute a dynamic SQL query (e.g., ALTER TABLE)
   * Use with caution - this bypasses prepared statement protections
   * Only use for schema modifications, never with user input
   */
  async executeDynamicSQL(sql: string): Promise<void> {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call initialize() first.');
    }

    try {
      await this.pool.query(sql);
      logger.debug('Dynamic SQL executed', { sql: sql.substring(0, 100) });
    } catch (error) {
      logger.error('Dynamic SQL execution failed', { sql, error });
      throw error;
    }
  }

  /**
   * Get current pool statistics
   */
  getPoolStats(): { total: number; idle: number; waiting: number } | null {
    if (!this.pool) return null;

    // mysql2 does not expose these pool internals in its public types.
    // We intentionally use (pool as any) here for observability in health checks.
    const pool = (this.pool as any).pool as any;

    return {
      total: pool?.config?.connectionLimit || 10,
      idle: pool?._freeConnections?.length || 0,
      waiting: pool?._connectionQueue?.length || 0,
    };
  }

  /**
   * Check if the database is connected
   */
  isHealthy(): boolean {
    return this.isConnected && this.pool !== null;
  }

  /**
   * Gracefully close the connection pool
   * Should be called during application shutdown
   */
  async close(): Promise<void> {
    if (this.pool) {
      try {
        await this.pool.end();
        this.pool = null;
        this.isConnected = false;
        logger.info('MySQL connection pool closed');
      } catch (error) {
        logger.error('Error closing MySQL connection pool', { error });
        throw error;
      }
    }
  }
}

// Export a singleton instance
export const db = new DatabaseManager();

// Also export the class for testing purposes
export { DatabaseManager };
