const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const redis = new Redis({
    host: '127.0.0.1',
    port: 6379,
});

function formatDateTime(value) {
    if (!value) return null;
    if (typeof value === 'string' && value.includes('T')) {
        return value.slice(0, 19).replace('T', ' ');
    }
    return value;
}

function normalizeDateTime(row) {
    const newRow = {};
    for (const [key, value] of Object.entries(row)) {
        if (key.includes('date') || key.includes('time') || key.includes('created') || key.includes('updated')) {
            newRow[key] = formatDateTime(value);
        } else {
            newRow[key] = value;
        }
    }
    return newRow;
}

(async () => {
    const targetDb = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'payment_db',
    });

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

        if (event === 'writerows') {
            const batchSize = 100;
            for (let i = 0; i < rows.length; i += batchSize) {
                const batch = rows.slice(i, i + batchSize);
                const normalizedBatch = batch.map(normalizeDateTime);

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
        } else if (event === 'updaterows') {
            for (const row of rows) {
                const newData = normalizeDateTime(row.after);
                const columns = Object.keys(newData);
                const values = Object.values(newData);

                const placeholders = columns.map(() => '?').join(', ');
                const sql = `REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;

                await targetDb.query(sql, values);
            }
            console.log(`[${new Date().toLocaleString()}] Updated ${rows.length} row(s) in ${table}`);
        } else if (event === 'deleterows') {
            for (const row of rows) {
                const id = row.id;
                const sql = `DELETE FROM ${table} WHERE id = ?`;
                await targetDb.query(sql, [id]);
            }
            console.log(`[${new Date().toLocaleString()}] Deleted ${rows.length} row(s) from ${table}`);
        }

        const end = Date.now();
        const elapsed = ((end - start) / 1000).toFixed(3);
        console.log(`[${new Date().toLocaleString()}] Process completed in ${elapsed} seconds`);
    });
})();
