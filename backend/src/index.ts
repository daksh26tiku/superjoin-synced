import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

import express, { Express, Request, Response, NextFunction } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { db } from './config/database';
import { redis } from './config/redis';
import { logger } from './utils/logger';
import { apiRouter } from './api/routes';
import { startSheetToDbWorker, stopSheetToDbWorker } from './queues/workers/sheetToDbWorker';
import { startDbToSheetWorker, stopDbToSheetWorker } from './queues/workers/dbToSheetWorker';
import { dbChangeWatcher } from './polling/DbChangeWatcher';
import { googleSheetsService } from './services/GoogleSheetsService';

/**
 * Main Application Entry Point
 * 
 * Initializes all services and starts the Express server.
 * Handles graceful shutdown for production reliability.
 */

const app: Express = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

// =============================================================================
// Middleware Configuration
// =============================================================================

// Security headers with CSP configured for dashboard
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      scriptSrcAttr: ["'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdn.tailwindcss.com"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'"],
      fontSrc: ["'self'"],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"],
    },
  },
}));

// CORS configuration
app.use(cors({
  origin: process.env.NODE_ENV === 'production'
    ? process.env.ALLOWED_ORIGINS?.split(',')
    : '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Webhook-Signature'],
}));

// Body parsing
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// API routes
app.use('/api', apiRouter);

// Request logging middleware
app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.http(`${req.method} ${req.path}`, {
    ip: req.ip,
    userAgent: req.get('user-agent'),
  });
  next();
});

// =============================================================================
// Health Check Endpoints
// =============================================================================

/**
 * Basic health check - returns 200 if server is running
 */
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/**
 * Deep health check - verifies all dependencies
 */
app.get('/health/deep', async (_req: Request, res: Response) => {
  const health: Record<string, unknown> = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    services: {},
  };

  let httpStatus = 200;

  // Check MySQL
  try {
    const dbHealthy = await db.testConnection();
    health.services = {
      ...health.services as object,
      mysql: {
        status: dbHealthy ? 'healthy' : 'unhealthy',
        pool: db.getPoolStats(),
      },
    };
    if (!dbHealthy) httpStatus = 503;
  } catch (error) {
    health.services = {
      ...health.services as object,
      mysql: { status: 'error', error: (error as Error).message },
    };
    httpStatus = 503;
  }

  // Check Redis
  try {
    const redisHealthy = await redis.testConnection();
    health.services = {
      ...health.services as object,
      redis: {
        status: redisHealthy ? 'healthy' : 'unhealthy',
      },
    };
    if (!redisHealthy) httpStatus = 503;
  } catch (error) {
    health.services = {
      ...health.services as object,
      redis: { status: 'error', error: (error as Error).message },
    };
    httpStatus = 503;
  }

  if (httpStatus !== 200) {
    health.status = 'degraded';
  }

  res.status(httpStatus).json(health);
});

// =============================================================================
// API Routes (Placeholder - will be added in next phase)
// =============================================================================

app.get('/api', (_req: Request, res: Response) => {
  res.json({
    name: 'Sheets-MySQL Sync API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      deepHealth: '/health/deep',
      webhook: '/api/webhook (coming soon)',
      syncStatus: '/api/sync/status (coming soon)',
      healthDashboard: '/api/health-dashboard',
    },
  });
});

// Dashboard route
app.get('/dashboard', (_req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, '../public/dashboard.html'));
});

// Get columns for a table (for dashboard column dropdown)
app.get('/api/tables/:tableName/columns', async (req: Request, res: Response) => {
  try {
    const { tableName } = req.params;

    const columns = await db.query<Array<{ COLUMN_NAME: string }>>(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = DATABASE() 
       AND TABLE_NAME = ? 
       AND COLUMN_NAME NOT LIKE '_sync%' 
       AND COLUMN_NAME NOT LIKE '_sheet%' 
       AND COLUMN_NAME NOT LIKE '_last%' 
       AND COLUMN_NAME NOT LIKE '_row%'
       ORDER BY ORDINAL_POSITION`,
      [tableName]
    );

    return res.json({
      success: true,
      columns: columns.map(c => c.COLUMN_NAME),
    });
  } catch (error) {
    logger.error('Failed to fetch columns', { error: (error as Error).message });
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch columns',
      error: (error as Error).message,
    });
  }
});

// Test Simulation Endpoint - Simulate Sheet Webhook
app.post('/api/test/simulate-webhook', async (req: Request, res: Response) => {
  try {
    // Extract dynamic values from request body with defaults
    const {
      sheetName: inputSheetName,
      row: inputRow,
      value: inputValue
    } = req.body || {};

    // Get sheet info from database (always need sheetId for the webhook)
    const sheets = await db.query<Array<{ sheet_name: string; sheet_id: string }>>(
      'SELECT sheet_name, sheet_id FROM sync_sheets LIMIT 1'
    );

    if (sheets.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'No sheets registered. Please register a Google Sheet first.',
      });
    }

    const sheetId = sheets[0].sheet_id;
    const sheetName = inputSheetName || sheets[0].sheet_name;

    // Create webhook payload with required sheetId
    const mockPayload = {
      sheetId: sheetId,
      sheetName: sheetName,
      row: inputRow || 99,
      col: 1,
      value: inputValue || 'Simulated Webhook Event',
      timestamp: new Date().toISOString(),
    };

    // Make HTTP POST to /api/webhook endpoint
    const webhookUrl = `http://localhost:${PORT}/api/webhook`;

    logger.info('Simulating webhook from dashboard', { payload: mockPayload });

    const webhookResponse = await fetch(webhookUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(mockPayload),
    });

    if (!webhookResponse.ok) {
      throw new Error(`Webhook returned ${webhookResponse.status}`);
    }

    return res.json({
      success: true,
      message: `Webhook triggered for ${sheetName}`,
      details: {
        sheetName,
        payload: mockPayload,
        webhookStatus: webhookResponse.status,
      },
    });
  } catch (error) {
    logger.error('Simulate webhook error', { error: (error as Error).message });
    return res.status(500).json({
      success: false,
      message: 'Failed to simulate webhook',
      error: (error as Error).message,
    });
  }
});

// Test Simulation Endpoint - Simulate DB Change
app.post('/api/test/simulate-db-change', async (req: Request, res: Response) => {
  try {
    // Extract dynamic values from request body
    const {
      tableName: inputTableName,
      rowId: inputRowId,
      colValue: inputColValue,
      colName: inputColName  // NEW: column name parameter
    } = req.body || {};

    // Get table info - use provided tableName or find first registered table
    let tableName = inputTableName;
    let sheetName = '';

    if (!inputTableName) {
      const tables = await db.query<Array<{ mysql_table_name: string; sheet_name: string }>>(
        'SELECT mysql_table_name, sheet_name FROM sync_sheets LIMIT 1'
      );
      if (tables.length === 0) {
        return res.status(404).json({
          success: false,
          message: 'No tables registered. Please register a Google Sheet first.',
        });
      }
      tableName = tables[0].mysql_table_name;
      sheetName = tables[0].sheet_name;
    } else {
      // Get sheet name for provided table
      const tableInfo = await db.query<Array<{ sheet_name: string }>>(
        'SELECT sheet_name FROM sync_sheets WHERE mysql_table_name = ?',
        [tableName]
      );
      sheetName = tableInfo[0]?.sheet_name || tableName;
    }

    // Get column to update - use provided colName or fall back to first data column
    let columnToUpdate = inputColName;

    if (!columnToUpdate) {
      const columns = await db.query<Array<{ COLUMN_NAME: string }>>(
        `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
         WHERE TABLE_SCHEMA = DATABASE() 
         AND TABLE_NAME = ? 
         AND COLUMN_NAME NOT LIKE '_sync%' 
         AND COLUMN_NAME NOT LIKE '_sheet%' 
         AND COLUMN_NAME NOT LIKE '_last%' 
         AND COLUMN_NAME NOT LIKE '_row%'
         ORDER BY ORDINAL_POSITION 
         LIMIT 1`,
        [tableName]
      );

      if (columns.length === 0) {
        return res.status(400).json({
          success: false,
          message: `Table ${tableName} has no data columns to update.`,
        });
      }
      columnToUpdate = columns[0].COLUMN_NAME;
    }

    const testValue = inputColValue || `Dashboard Test ${new Date().toISOString()}`;

    if (inputRowId) {
      // UPDATE mode: Update specific row by _sync_row_id
      await db.query(
        `UPDATE \`${tableName}\` 
         SET \`${columnToUpdate}\` = ?, _last_modified_by = 'DATABASE', _sync_status = 'PENDING', _last_modified_at = NOW() 
         WHERE _sync_row_id = ?`,
        [testValue, inputRowId]
      );

      logger.info('Test row updated via dashboard', {
        table: tableName,
        column: columnToUpdate,
        value: testValue,
        rowId: inputRowId,
      });

      return res.json({
        success: true,
        message: `Row ${inputRowId} updated in ${tableName}`,
        details: {
          table: tableName,
          sheet: sheetName,
          column: columnToUpdate,
          value: testValue,
          rowId: inputRowId,
          operation: 'UPDATE',
        },
      });
    } else {
      // INSERT mode: Create new row (original behavior)
      const maxRowResult = await db.query<Array<{ max_row: number | null }>>(
        `SELECT MAX(_sheet_row_number) as max_row FROM \`${tableName}\``
      );
      const nextRowNumber = (maxRowResult[0]?.max_row || 0) + 1;

      await db.query(
        `INSERT INTO \`${tableName}\` 
         (\`${columnToUpdate}\`, _sheet_row_number, _last_modified_by, _sync_status, _last_modified_at) 
         VALUES (?, ?, 'DATABASE', 'PENDING', NOW())`,
        [testValue, nextRowNumber]
      );

      logger.info('Test row inserted via dashboard', {
        table: tableName,
        column: columnToUpdate,
        value: testValue,
        rowNumber: nextRowNumber,
      });

      return res.json({
        success: true,
        message: `Row inserted into ${tableName}`,
        details: {
          table: tableName,
          sheet: sheetName,
          column: columnToUpdate,
          value: testValue,
          rowNumber: nextRowNumber,
          operation: 'INSERT',
        },
      });
    }
  } catch (error) {
    logger.error('Simulate DB change error', { error: (error as Error).message });
    return res.status(500).json({
      success: false,
      message: 'Failed to simulate database change',
      error: (error as Error).message,
    });
  }
});

// Health Dashboard API
app.get('/api/health-dashboard', async (_req: Request, res: Response) => {
  try {
    const response: {
      status: string;
      activeWorkers: number;
      lastSync: string;
      totalRows: number;
      feed: Array<{ type: string; message: string; timestamp: string }>;
      registry: Array<{ sheet_name: string; sheet_id: string; mysql_table_name: string; created_at: string }>;
    } = {
      status: 'online',
      activeWorkers: 3,
      lastSync: 'Unknown',
      totalRows: 0,
      feed: [],
      registry: [],
    };

    // Query feed from sync_audit_log
    try {
      const feedRows = await db.query<Array<{
        operation_type: string;
        details: string | null;
        created_at: Date;
      }>>(
        'SELECT operation_type, details, created_at FROM sync_audit_log ORDER BY created_at DESC LIMIT 10'
      );

      response.feed = feedRows.map(row => ({
        type: row.operation_type,
        message: row.details || 'No details',
        timestamp: row.created_at instanceof Date
          ? row.created_at.toISOString()
          : new Date(row.created_at).toISOString(),
      }));

      // Calculate lastSync from most recent entry
      if (feedRows.length > 0) {
        const lastSyncTime = new Date(feedRows[0].created_at);
        const now = new Date();
        const diffMs = now.getTime() - lastSyncTime.getTime();
        const diffSeconds = Math.floor(diffMs / 1000);

        if (diffSeconds < 60) {
          response.lastSync = `${diffSeconds} seconds ago`;
        } else if (diffSeconds < 3600) {
          response.lastSync = `${Math.floor(diffSeconds / 60)} minutes ago`;
        } else if (diffSeconds < 86400) {
          response.lastSync = `${Math.floor(diffSeconds / 3600)} hours ago`;
        } else {
          response.lastSync = `${Math.floor(diffSeconds / 86400)} days ago`;
        }
      }
    } catch (error) {
      logger.error('Failed to fetch feed from sync_audit_log', { error: (error as Error).message });
      response.feed = [];
    }

    // Query total rows from sync_audit_log
    try {
      const countRows = await db.query<Array<{ cnt: number }>>(
        'SELECT COUNT(*) as cnt FROM sync_audit_log'
      );
      response.totalRows = countRows[0]?.cnt || 0;
    } catch (error) {
      logger.error('Failed to count rows in sync_audit_log', { error: (error as Error).message });
      response.totalRows = 0;
    }

    // Query registry from sync_sheets
    try {
      const registryRows = await db.query<Array<{
        sheet_name: string;
        sheet_id: string;
        mysql_table_name: string;
        created_at: Date;
      }>>(
        'SELECT sheet_name, sheet_id, mysql_table_name, created_at FROM sync_sheets'
      );

      response.registry = registryRows.map(row => ({
        sheet_name: row.sheet_name,
        sheet_id: row.sheet_id,
        mysql_table_name: row.mysql_table_name,
        created_at: row.created_at instanceof Date
          ? row.created_at.toISOString()
          : new Date(row.created_at).toISOString(),
      }));
    } catch (error) {
      logger.error('Failed to fetch registry from sync_sheets', { error: (error as Error).message });
      response.registry = [];
    }

    res.json(response);
  } catch (error) {
    logger.error('Health dashboard API error', { error: (error as Error).message });
    res.status(500).json({
      status: 'error',
      activeWorkers: 0,
      lastSync: 'Unknown',
      totalRows: 0,
      feed: [],
      registry: [],
    });
  }
});

// =============================================================================
// Error Handling
// =============================================================================

// 404 handler
app.use((_req: Request, res: Response) => {
  res.status(404).json({
    error: 'Not Found',
    message: 'The requested endpoint does not exist',
  });
});

// Global error handler
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error('Unhandled error', { error: err.message, stack: err.stack });

  res.status(500).json({
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'production'
      ? 'An unexpected error occurred'
      : err.message,
  });
});

// =============================================================================
// Server Initialization
// =============================================================================

async function startServer(): Promise<void> {
  try {
    logger.info('Starting Sheets-MySQL Sync Backend...');

    // Initialize database connection
    logger.info('Connecting to MySQL...');
    await db.initialize();

    // Initialize Redis connection
    logger.info('Connecting to Redis...');
    await redis.initialize();

    // Initialize Google Sheets service (will warn if no credentials)
    await googleSheetsService.initialize();

    // Start BullMQ workers after Redis is ready
    startSheetToDbWorker();
    startDbToSheetWorker();

    // Start DB change polling (5-second interval)
    dbChangeWatcher.start();

    // Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`, {
        env: process.env.NODE_ENV || 'development',
        port: PORT,
      });
    });

    // Graceful shutdown handlers
    const gracefulShutdown = async (signal: string): Promise<void> => {
      logger.info(`${signal} received. Starting graceful shutdown...`);

      // Stop accepting new connections
      server.close(async () => {
        logger.info('HTTP server closed');

        try {
          // Stop polling first
          dbChangeWatcher.stop();

          // Stop workers
          await stopSheetToDbWorker();
          await stopDbToSheetWorker();

          // Close database connections
          await db.close();
          logger.info('Database connections closed');

          // Close Redis connections
          await redis.close();
          logger.info('Redis connections closed');

          logger.info('Graceful shutdown completed');
          process.exit(0);
        } catch (error) {
          logger.error('Error during graceful shutdown', { error });
          process.exit(1);
        }
      });

      // Force shutdown after 30 seconds
      setTimeout(() => {
        logger.error('Graceful shutdown timeout. Forcing exit...');
        process.exit(1);
      }, 30000);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

  } catch (error) {
    logger.error('Failed to start server', { error });
    process.exit(1);
  }
}

// Start the server
startServer();

export default app;
