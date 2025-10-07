require('dotenv').config();
const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const {
    REDIS_HOST,
    REDIS_PORT,
    MYSQL_HOST_MIRROR,
    MYSQL_USER_MIRROR,
    MYSQL_PASSWORD_MIRROR,
    MYSQL_DB_MIRROR,
} = process.env;

const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
});

const MYSQL_CONFIG = {
    host: MYSQL_HOST_MIRROR,
    user: MYSQL_USER_MIRROR,
    password: MYSQL_PASSWORD_MIRROR,
    database: MYSQL_DB_MIRROR,
};

function sanitizeRow(row) {
    const newRow = {};
    for (const [key, value] of Object.entries(row)) {
        if (value === null || value === undefined) {
            newRow[key] = null;
        }
        else if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
            newRow[key] = value.slice(0, 19).replace('T', ' ');
        }
        else {
            newRow[key] = value;
        }
    }
    return newRow;
}

(async () => {
    const targetDb = await mysql.createConnection(MYSQL_CONFIG);

    console.log(`[${new Date().toLocaleString()}] CDC CONSUMER STARTED - Listening for Redis events`);
    const subscriber = redis.duplicate();

    await subscriber.subscribe('cdc_events', async (err) => {
        if (err) throw err;
        console.log(`[${new Date().toLocaleString()}] Subscribed to cdc_events channel`);
    });

    subscriber.on('message', async (channel, message) => {
        const start = Date.now();
        const data = JSON.parse(message);
        const { table, event, rows } = data;

        console.log(`[${new Date().toLocaleString()}] Event received: ${event.toUpperCase()} on table "${table}" (${rows.length} rows)`);

        try {
            if (event === 'writerows') {
                const batchSize = 100;
                for (let i = 0; i < rows.length; i += batchSize) {
                    const batch = rows.slice(i, i + batchSize);
                    const normalizedBatch = batch.map(sanitizeRow);

                    if (normalizedBatch.length === 0) continue;

                    const columns = Object.keys(normalizedBatch[0]);
                    const placeholders = normalizedBatch.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                    const values = normalizedBatch.flatMap(Object.values);

                    const sql = `
                        INSERT INTO ${table} (${columns.join(', ')}) VALUES ${placeholders}
                        ON DUPLICATE KEY UPDATE ${columns.map(col => `${col}=VALUES(${col})`).join(', ')}
                    `;

                    await targetDb.query(sql, values);
                    console.log(`[${new Date().toLocaleString()}] Batch of ${normalizedBatch.length} inserted/updated in ${table}`);
                }
            }
            else if (event === 'updaterows') {
                for (const row of rows) {
                    const newData = sanitizeRow(row.after);
                    const columns = Object.keys(newData);
                    const values = Object.values(newData);

                    const placeholders = columns.map(() => '?').join(', ');
                    const sql = `REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;

                    await targetDb.query(sql, values);
                }
                console.log(`[${new Date().toLocaleString()}] Updated ${rows.length} row(s) in ${table}`);
            }
            else if (event === 'deleterows') {
                for (const row of rows) {
                    const id = row.id;
                    const sql = `DELETE FROM ${table} WHERE id = ?`;
                    await targetDb.query(sql, [id]);
                }
                console.log(`[${new Date().toLocaleString()}] Deleted ${rows.length} row(s) from ${table}`);
            }
        } catch (err) {
            console.error(`[${new Date().toLocaleString()}] Error processing event:`, err.message);
        }

        const elapsed = ((Date.now() - start) / 1000).toFixed(3);
        console.log(`[${new Date().toLocaleString()}] Process completed in ${elapsed} seconds`);
    });
})();
