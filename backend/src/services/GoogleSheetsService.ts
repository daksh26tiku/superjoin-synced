import { google, sheets_v4 } from 'googleapis';
import path from 'path';
import { logger } from '../utils/logger';

interface SheetUpdatePayload {
  spreadsheetId: string;
  sheetName: string;
  row: number;
  values: Record<string, unknown>;
  columnMapping: Record<string, string>; // mysqlColumn -> sheetColumn (A, B, C...)
}

interface BatchUpdatePayload {
  spreadsheetId: string;
  sheetName: string;
  updates: Array<{
    row: number;
    values: Record<string, unknown>;
    columnMapping: Record<string, string>;
  }>;
}

class GoogleSheetsService {
  private sheets: sheets_v4.Sheets | null = null;
  private initialized = false;

  /**
   * Initialize the Google Sheets API client.
   * Call this with proper credentials before using other methods.
   * 
   * For Service Account auth:
   *   const auth = new google.auth.GoogleAuth({
   *     keyFile: '/path/to/service-account.json',
   *     scopes: ['https://www.googleapis.com/auth/spreadsheets'],
   *   });
   * 
   * For OAuth2:
   *   const auth = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
   *   auth.setCredentials({ refresh_token: '...' });
   */
  async initialize(auth?: any): Promise<void> {
    if (this.initialized) {
      return;
    }

    try {
      // Placeholder: Use provided auth or create from environment
      const authClient = auth || this.createAuthFromEnv();

      this.sheets = google.sheets({ version: 'v4', auth: authClient });
      this.initialized = true;

      logger.info('GoogleSheetsService initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize GoogleSheetsService', { error });
      throw error;
    }
  }

  /**
   * Create auth client from environment variables.
   * Supports Service Account (JSON key), OAuth2, and API Key.
   */
  private createAuthFromEnv(): any {
    // Check for API key first (simplest)
    const apiKey = process.env.GOOGLE_API_KEY;
    if (apiKey) {
      logger.info('Using Google API Key authentication');
      return apiKey; // The google client library accepts API keys as auth
    }

    const serviceAccountPath = process.env.GOOGLE_SERVICE_ACCOUNT_PATH;
    const clientEmail = process.env.GOOGLE_CLIENT_EMAIL;
    const privateKey = process.env.GOOGLE_PRIVATE_KEY;

    if (serviceAccountPath) {
      // Resolve path relative to backend directory
      const resolvedPath = path.resolve(__dirname, '../../', serviceAccountPath);
      logger.info('Using Google service account', { path: resolvedPath });
      return new google.auth.GoogleAuth({
        keyFile: resolvedPath,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      });
    }

    if (clientEmail && privateKey) {
      // Use inline credentials (useful for deployment)
      return new google.auth.GoogleAuth({
        credentials: {
          client_email: clientEmail,
          private_key: privateKey.replace(/\\n/g, '\n'),
        },
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      });
    }

    // Return null auth for now - will fail on actual API calls
    // This allows the service to start without credentials for development
    logger.warn('No Google credentials configured. Sheet updates will fail.');
    return null;
  }

  /**
   * Update a single row in a Google Sheet.
   */
  async updateSheetRow(payload: SheetUpdatePayload): Promise<void> {
    if (!this.sheets) {
      throw new Error('GoogleSheetsService not initialized. Call initialize() first.');
    }

    const { spreadsheetId, sheetName, row, values, columnMapping } = payload;

    // Build the data array for batchUpdate
    const data: sheets_v4.Schema$ValueRange[] = [];

    for (const [mysqlCol, sheetCol] of Object.entries(columnMapping)) {
      if (values[mysqlCol] !== undefined) {
        const range = `${sheetName}!${sheetCol}${row}`;
        const cellValue = this.formatValueForSheet(values[mysqlCol]);

        data.push({
          range,
          values: [[cellValue]],
        });
      }
    }

    if (data.length === 0) {
      logger.debug('No values to update for row', { spreadsheetId, row });
      return;
    }

    try {
      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info('Sheet row updated successfully', {
        spreadsheetId,
        sheetName,
        row,
        cellCount: data.length,
      });
    } catch (error: any) {
      logger.error('Failed to update sheet row', {
        spreadsheetId,
        sheetName,
        row,
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Batch update multiple rows in a Google Sheet.
   * More efficient than calling updateSheetRow multiple times.
   */
  async batchUpdateRows(payload: BatchUpdatePayload): Promise<void> {
    if (!this.sheets) {
      throw new Error('GoogleSheetsService not initialized. Call initialize() first.');
    }

    const { spreadsheetId, sheetName, updates } = payload;
    const data: sheets_v4.Schema$ValueRange[] = [];

    for (const update of updates) {
      const { row, values, columnMapping } = update;

      for (const [mysqlCol, sheetCol] of Object.entries(columnMapping)) {
        if (values[mysqlCol] !== undefined) {
          const range = `${sheetName}!${sheetCol}${row}`;
          const cellValue = this.formatValueForSheet(values[mysqlCol]);

          data.push({
            range,
            values: [[cellValue]],
          });
        }
      }
    }

    if (data.length === 0) {
      logger.debug('No values to batch update', { spreadsheetId });
      return;
    }

    try {
      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data,
        },
      });

      logger.info('Sheet batch update successful', {
        spreadsheetId,
        sheetName,
        rowCount: updates.length,
        cellCount: data.length,
      });
    } catch (error: any) {
      logger.error('Failed to batch update sheet', {
        spreadsheetId,
        sheetName,
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Read values from a sheet range.
   * Useful for fetching current state for conflict detection.
   */
  async readRange(
    spreadsheetId: string,
    range: string
  ): Promise<any[][] | null> {
    if (!this.sheets) {
      throw new Error('GoogleSheetsService not initialized. Call initialize() first.');
    }

    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId,
        range,
      });

      return response.data.values || null;
    } catch (error: any) {
      logger.error('Failed to read sheet range', {
        spreadsheetId,
        range,
        error: error.message,
      });
      throw error;
    }
  }

  /**
   * Format a value for Google Sheets.
   * Handles dates, numbers, booleans, and nulls.
   */
  private formatValueForSheet(value: unknown): string | number | boolean {
    if (value === null || value === undefined) {
      return '';
    }

    if (value instanceof Date) {
      return value.toISOString();
    }

    if (typeof value === 'boolean') {
      return value;
    }

    if (typeof value === 'number') {
      return value;
    }

    return String(value);
  }

  /**
   * Check if the service is properly initialized with credentials.
   */
  isInitialized(): boolean {
    return this.initialized && this.sheets !== null;
  }
}

// Singleton instance
export const googleSheetsService = new GoogleSheetsService();
