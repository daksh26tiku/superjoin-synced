import { Job, Worker } from 'bullmq';
import { RowDataPacket } from 'mysql2/promise';
import { v4 as uuidv4 } from 'uuid';
import { db } from '../../config/database';
import { redis } from '../../config/redis';
import { logger } from '../../utils/logger';
import { computeRowHash } from '../../utils/hash';
import { coerceValue, inferColumnType } from '../../utils/typeCoercion';
import { schemaManager } from '../../services/SchemaManager';
import { SHEET_TO_DB_QUEUE_NAME } from '../sheetToDbQueue';
import { SheetToDbJobData, SyncedRow } from '../../types';

let worker: Worker<SheetToDbJobData> | null = null;

function toMysqlDateTime(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

async function processJob(job: Job<SheetToDbJobData>): Promise<void> {
  const { sheetId, sheetName, row, col, value, timestamp } = job.data;

  const lockKey = `sync:lock:${sheetId}:${sheetName}:${row}:${col}`;
  const lockValue = uuidv4();
  const lockAcquired = await redis.acquireLock(lockKey, 10000, lockValue);

  if (!lockAcquired) {
    throw new Error(`Lock not acquired for ${lockKey}`);
  }

  try {
    const incomingDate = new Date(timestamp);
    if (Number.isNaN(incomingDate.getTime())) {
      throw new Error('Invalid timestamp in job payload');
    }

    const inferredType = inferColumnType(value);

    const { tableName } = await schemaManager.ensureSheetRegistration(sheetId, sheetName);
    const mapping = await schemaManager.ensureColumnMapping(sheetId, sheetName, col, inferredType);
    await schemaManager.ensureColumnExistsOnTable(tableName, mapping.mysqlColumnName, mapping.dataType);

    const mysqlValue = coerceValue(value, mapping.dataType);

    await db.transaction(async (connection) => {
      type LastModifiedRow = RowDataPacket & { _last_modified_at: string | Date };
      const [existingRows] = await connection.query<LastModifiedRow[]>(
        `SELECT _last_modified_at FROM \`${tableName}\` WHERE _sheet_row_number = ? LIMIT 1`,
        [row]
      );

      if (existingRows.length > 0) {
        const existingTs = new Date(existingRows[0]._last_modified_at);
        if (!Number.isNaN(existingTs.getTime()) && existingTs.getTime() > incomingDate.getTime()) {
          logger.warn('Skipping stale Sheet update (LWW)', {
            sheetId,
            sheetName,
            row,
            col,
            incoming: incomingDate.toISOString(),
            existing: existingTs.toISOString(),
          });
          return;
        }
      }

      const lastModifiedAt = toMysqlDateTime(incomingDate);

      await connection.query(
        `INSERT INTO \`${tableName}\` (
           _sheet_row_number,
           _last_modified_at,
           _last_modified_by,
           _sync_status,
           \`${mapping.mysqlColumnName}\`
         ) VALUES (?, ?, 'SHEET', 'SYNCED', ?)
         ON DUPLICATE KEY UPDATE
           _last_modified_at = VALUES(_last_modified_at),
           _last_modified_by = 'SHEET',
           _sync_status = 'SYNCED',
           \`${mapping.mysqlColumnName}\` = VALUES(\`${mapping.mysqlColumnName}\`)`,
        [row, lastModifiedAt, mysqlValue]
      );

      type SyncedRowPacket = RowDataPacket & SyncedRow;
      const [rowAfter] = await connection.query<SyncedRowPacket[]>(
        `SELECT * FROM \`${tableName}\` WHERE _sheet_row_number = ? LIMIT 1`,
        [row]
      );

      if (rowAfter.length > 0) {
        const { _row_hash: _ignored, ...hashable } = rowAfter[0] as Record<string, unknown>;
        const newHash = computeRowHash(hashable);

        await connection.query(
          `UPDATE \`${tableName}\` SET _row_hash = ? WHERE _sheet_row_number = ?`,
          [newHash, row]
        );
      }
    });

    logger.info('Applied Sheet change to DB', {
      sheetId,
      sheetName,
      row,
      col,
      mysqlColumn: mapping.mysqlColumnName,
    });

    // Log to sync_audit_log for Live Event Feed
    await db.execute(
      `INSERT INTO sync_audit_log (operation_type, details, created_at)
       VALUES ('SHEET_TO_DB', ?, NOW())`,
      [JSON.stringify({ message: `Row ${row} col ${col} updated from ${sheetName}`, value: String(value).substring(0, 50), sheetId })]
    );
  } finally {
    await redis.releaseLock(lockKey, lockValue);
  }
}

export function startSheetToDbWorker(): Worker<SheetToDbJobData> {
  if (worker) return worker;

  worker = new Worker<SheetToDbJobData>(SHEET_TO_DB_QUEUE_NAME, processJob, {
    connection: redis.createConnection(),
    concurrency: 5,
  });

  worker.on('failed', (job, err) => {
    logger.error('sheet-to-db job failed', {
      jobId: job?.id,
      name: job?.name,
      error: err.message,
    });
  });

  worker.on('completed', (job) => {
    logger.debug('sheet-to-db job completed', { jobId: job.id });
  });

  return worker;
}

export async function stopSheetToDbWorker(): Promise<void> {
  if (!worker) return;
  await worker.close();
  worker = null;
}
