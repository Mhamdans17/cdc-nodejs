const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const redis = new Redis({
    host: '127.0.0.1',
    port: 6379,
});

(async () => {
    const targetDb = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'replica_db',
    });

    console.log('🔄 CDC Consumer started. Listening for Redis events...');

    const subscriber = redis.duplicate();

    await subscriber.subscribe('cdc_events', async (err, count) => {
        if (err) throw err;
        console.log('📡 Subscribed to cdc_events channel');
    });

    subscriber.on('message', async (channel, message) => {
        const data = JSON.parse(message);
        const { table, event, rows } = data;

        if (event === 'writerows') {
            for (const row of rows) {
                const columns = Object.keys(row);
                const values = Object.values(row);

                const placeholders = columns.map(() => '?').join(', ');
                const sql = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})
          ON DUPLICATE KEY UPDATE ${columns.map(col => `${col}=VALUES(${col})`).join(', ')}`;

                await targetDb.query(sql, values);
                console.log(`🟢 Inserted/Updated in replica_db.${table}`);
            }
        } else if (event === 'updaterows') {
            for (const row of rows) {
                const newData = row.after;
                const columns = Object.keys(newData);
                const values = Object.values(newData);

                const placeholders = columns.map(() => '?').join(', ');
                const sql = `REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;

                await targetDb.query(sql, values);
                console.log(`🟡 Updated in replica_db.${table}`);
            }
        } else if (event === 'deleterows') {
            for (const row of rows) {
                const id = row.id;
                const sql = `DELETE FROM ${table} WHERE id = ?`;
                await targetDb.query(sql, [id]);
                console.log(`🔴 Deleted from replica_db.${table}`);
            }
        }
    });
})();
