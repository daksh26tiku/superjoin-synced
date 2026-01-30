import { db } from '../config/database';
import { getDbToSheetQueue } from '../queues/dbToSheetQueue';
import { logger } from '../utils/logger';
import { RowDataPacket } from 'mysql2/promise';

interface PendingChange extends RowDataPacket {
  _sync_row_id: string;
  _sheet_row_number: number;
  _sync_status: string;
  _last_modified_by: string;
  _last_modified_at: Date;
  [key: string]: unknown;
}

interface SheetRegistry extends RowDataPacket {
  sheet_id: string;
  sheet_name: string;
  table_name: string;
}

interface ColumnMapping extends RowDataPacket {
  mysql_column_name: string;
  column_index: number;
}

class DbChangeWatcher {
  private intervalId: NodeJS.Timeout | null = null;
  private isRunning = false;
  private pollIntervalMs: number;

  constructor(pollIntervalMs: number = 5000) {
    this.pollIntervalMs = pollIntervalMs;
  }

  /**
   * Start the polling loop.
   */
  start(): void {
    if (this.isRunning) {
      logger.warn('DbChangeWatcher is already running');
      return;
    }

    this.isRunning = true;
    logger.info('DbChangeWatcher started', { pollIntervalMs: this.pollIntervalMs });

    // Run immediately, then on interval
    this.poll();
    this.intervalId = setInterval(() => this.poll(), this.pollIntervalMs);
  }

  /**
   * Stop the polling loop.
   */
  stop(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.isRunning = false;
    logger.info('DbChangeWatcher stopped');
  }

  /**
   * Single poll iteration.
   * Finds pending DB changes and enqueues them for Sheet sync.
   */
  private async poll(): Promise<void> {
    try {
      // Get all registered sheets/tables
      const registeredSheets = await this.getRegisteredSheets();

      for (const sheet of registeredSheets) {
        await this.processPendingChanges(sheet);
      }
    } catch (error) {
      logger.error('DbChangeWatcher poll error', { error });
    }
  }

  /**
   * Get all registered sheet-table mappings.
   */
  private async getRegisteredSheets(): Promise<SheetRegistry[]> {
    const rows = await db.query<SheetRegistry[]>(
      'SELECT sheet_id, sheet_name, mysql_table_name as table_name FROM sync_sheets WHERE sync_enabled = TRUE'
    );
    return rows;
  }

  /**
   * Find and process pending changes for a specific table.
   * Only processes rows where:
   *   - _sync_status = 'PENDING'
   *   - _last_modified_by = 'DATABASE'
   * This prevents infinite loops by ignoring Sheet-originated changes.
   */
  private async processPendingChanges(sheet: SheetRegistry): Promise<void> {
    const { sheet_id, table_name } = sheet;

    try {
      // Check if table exists
      const tables = await db.query<RowDataPacket[]>(
        `SELECT TABLE_NAME FROM information_schema.TABLES 
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
        [table_name]
      );

      if (tables.length === 0) {
        return; // Table doesn't exist yet
      }

      // Find pending changes from DATABASE side
      const pendingRows = await db.query<PendingChange[]>(
        `SELECT * FROM \`${table_name}\` 
         WHERE _sync_status = 'PENDING' 
         AND _last_modified_by = 'DATABASE'
         LIMIT 100`,
      );

      if (pendingRows.length === 0) {
        return;
      }

      logger.info('Found pending DB changes', {
        table: table_name,
        count: pendingRows.length,
      });

      // Get column mappings for this sheet
      const columnMapping = await this.getColumnMapping(sheet_id);

      // Enqueue each change
      for (const row of pendingRows) {
        await this.enqueueChange(sheet, row, columnMapping);
      }
    } catch (error: any) {
      // Ignore if table doesn't have sync columns yet
      if (error.code === 'ER_BAD_FIELD_ERROR') {
        return;
      }
      logger.error('Error processing pending changes', {
        table: table_name,
        error: error.message,
      });
    }
  }

  /**
   * Get column mappings for a sheet (MySQL column -> Sheet column letter).
   */
  private async getColumnMapping(sheetId: string): Promise<Record<string, string>> {
    const rows = await db.query<ColumnMapping[]>(
      `SELECT mysql_column_name, column_index 
       FROM sync_schema_registry 
       WHERE sheet_id = ?`,
      [sheetId]
    );

    const mapping: Record<string, string> = {};
    for (const row of rows) {
      // Convert column_index to sheet column letter (0 -> A, 1 -> B, etc.)
      const sheetColumn = this.columnIndexToLetter(row.column_index);
      mapping[row.mysql_column_name] = sheetColumn;
    }
    return mapping;
  }

  /**
   * Convert column index to Excel-style column letter.
   * 0 -> A, 1 -> B, 25 -> Z, 26 -> AA, etc.
   */
  private columnIndexToLetter(index: number): string {
    let letter = '';
    let num = index;
    
    while (num >= 0) {
      letter = String.fromCharCode(65 + (num % 26)) + letter;
      num = Math.floor(num / 26) - 1;
    }
    
    return letter;
  }

  /**
   * Enqueue a single row change to the db-to-sheet queue.
   * Also marks the row as SYNCING to prevent duplicate processing.
   */
  private async enqueueChange(
    sheet: SheetRegistry,
    row: PendingChange,
    columnMapping: Record<string, string>
  ): Promise<void> {
    const { sheet_id, sheet_name, table_name } = sheet;
    const rowId = row._sync_row_id;
    const sheetRow = row._sheet_row_number;

    // Extract user data columns (exclude sync metadata)
    const values: Record<string, unknown> = {};
    for (const key of Object.keys(row)) {
      if (!key.startsWith('_')) {
        values[key] = row[key];
      }
    }

    try {
      // Mark as SYNCING to prevent re-processing
      await db.query(
        `UPDATE \`${table_name}\` SET _sync_status = 'SYNCING' WHERE _sync_row_id = ?`,
        [rowId]
      );

      // Enqueue the job
      await getDbToSheetQueue().add(
        `sync-${sheet_id}-row-${sheetRow}`,
        {
          tableName: table_name,
          sheetId: sheet_id,
          sheetName: sheet_name,
          rowId,
          row: sheetRow,
          values,
          columnMapping,
          timestamp: new Date().toISOString(),
        },
        {
          jobId: `db-to-sheet-${rowId}-${Date.now()}`,
        }
      );

      logger.debug('Enqueued DB->Sheet sync job', {
        sheetId: sheet_id,
        rowId,
        sheetRow,
      });
    } catch (error) {
      // Revert to PENDING on failure
      await db.query(
        `UPDATE \`${table_name}\` SET _sync_status = 'PENDING' WHERE _sync_row_id = ?`,
        [rowId]
      );
      throw error;
    }
  }

  /**
   * Check if the watcher is currently running.
   */
  isWatching(): boolean {
    return this.isRunning;
  }
}

// Singleton instance with 5-second polling interval
export const dbChangeWatcher = new DbChangeWatcher(5000);
