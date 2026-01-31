import dotenv from 'dotenv';
import path from 'path';
import { readFileSync } from 'fs';
import mysql from 'mysql2/promise';

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

/**
 * Database Initialization Script
 * 
 * This script runs the SQL initialization file to create all required tables
 * for the sheets-mysql-sync system. It's designed to be idempotent - safe to
 * run multiple times as it uses CREATE TABLE IF NOT EXISTS.
 * 
 * Usage: node dist/scripts/initDatabase.js
 */

interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl?: {};
}

async function initializeDatabase(): Promise<void> {
  const config: DatabaseConfig = {
    host: process.env.MYSQL_HOST || 'localhost',
    port: parseInt(process.env.MYSQL_PORT || '3307', 10),
    user: process.env.MYSQL_USER || 'sync_user',
    password: process.env.MYSQL_PASSWORD || 'sync_password',
    database: process.env.MYSQL_DATABASE || 'defaultdb',
    ssl: process.env.MYSQL_CA_CERT ? { ca: process.env.MYSQL_CA_CERT } : undefined,
  };

  console.log('🔧 Database Initialization Script');
  console.log('================================');
  console.log(`Host: ${config.host}`);
  console.log(`Port: ${config.port}`);
  console.log(`Database: ${config.database}`);
  console.log(`User: ${config.user}`);
  console.log('');

  let connection;

  try {
    // Create connection 
    connection = await mysql.createConnection(config);
    console.log('✅ Connected to MySQL database');

    // Read the SQL initialization file
    const sqlFilePath = path.resolve(__dirname, '../../sql/init/001_init_schema.sql');
    console.log(`📄 Reading SQL file: ${sqlFilePath}`);
    const sqlContent = readFileSync(sqlFilePath, 'utf-8');

    // Split SQL file into individual statements
    // Remove comments and split by semicolon
    const statements = sqlContent
      .split('\n')
      .filter(line => !line.trim().startsWith('--') && line.trim() !== '')
      .join('\n')
      .split(';')
      .map(stmt => stmt.trim())
      .filter(stmt => stmt.length > 0 && !stmt.startsWith('DELIMITER'));

    console.log(`📋 Found ${statements.length} SQL statements to execute\n`);

    // Execute each statement
    let successCount = 0;
    let skipCount = 0;

    for (let i = 0; i < statements.length; i++) {
      const statement = statements[i];
      
      // Skip certain statements that might not be compatible
      if (statement.includes('USE sheets_sync') || 
          statement.includes('GRANT ALL PRIVILEGES') ||
          statement.includes('CREATE EVENT') ||
          statement.includes('SET GLOBAL event_scheduler')) {
        console.log(`⏭️  Skipping statement ${i + 1}: ${statement.substring(0, 50)}...`);
        skipCount++;
        continue;
      }

      try {
        await connection.query(statement);
        
        // Extract table/entity name for better logging
        let entityName = 'unknown';
        if (statement.includes('CREATE TABLE')) {
          const match = statement.match(/CREATE TABLE.*?`?(\w+)`?/i);
          if (match) entityName = match[1];
        }
        
        console.log(`✅ Statement ${i + 1}/${statements.length}: ${entityName}`);
        successCount++;
      } catch (error: any) {
        // Ignore "already exists" errors since we use IF NOT EXISTS
        if (error.code === 'ER_TABLE_EXISTS_ERROR' || error.errno === 1050) {
          console.log(`⚠️  Statement ${i + 1}: Table already exists (skipped)`);
          skipCount++;
        } else {
          console.error(`❌ Failed to execute statement ${i + 1}:`, error.message);
          console.error(`Statement: ${statement.substring(0, 100)}...`);
          throw error;
        }
      }
    }

    console.log('');
    console.log('================================');
    console.log('🎉 Database initialization complete!');
    console.log(`✅ Successful: ${successCount}`);
    console.log(`⏭️  Skipped: ${skipCount}`);
    console.log('================================');

    // Verify tables were created
    console.log('\n📊 Verifying table creation...');
    const [tables] = await connection.query(
      `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
       WHERE TABLE_SCHEMA = ? 
       ORDER BY TABLE_NAME`,
      [config.database]
    );

    console.log('\nTables in database:');
    (tables as any[]).forEach((table: any) => {
      console.log(`  - ${table.TABLE_NAME}`);
    });

  } catch (error: any) {
    console.error('\n❌ Database initialization failed:', error.message);
    console.error(error);
    process.exit(1);
  } finally {
    if (connection) {
      await connection.end();
      console.log('\n🔌 Database connection closed');
    }
  }
}

// Run initialization
initializeDatabase().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
