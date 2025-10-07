require('dotenv').config();
const Redis = require('ioredis');
const oracledb = require('oracledb');

oracledb.fetchAsString = [oracledb.CLOB];

const {
    REDIS_HOST,
    REDIS_PORT,
    ORACLE_USER,
    ORACLE_PASSWORD,
    ORACLE_CONNECT_STRING
} = process.env;

const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
});

function sanitizeRow(row) {
    const newRow = {};
    for (const [key, value] of Object.entries(row)) {
        if (value === null || value === undefined) {
            newRow[key] = null;
        } else if (
            typeof value === 'string' &&
            /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/.test(value)
        ) {
            newRow[key] = value.replace('T', ' ').slice(0, 19);
        } else {
            newRow[key] = value;
        }
    }
    return newRow;
}

(async () => {
    try {
        const oracleConn = await oracledb.getConnection({
            user: ORACLE_USER,
            password: ORACLE_PASSWORD,
            connectString: ORACLE_CONNECT_STRING
        });

        console.log(`[${new Date().toLocaleString()}] ✅ CDC CONSUMER (Oracle) STARTED`);

        const subscriber = redis.duplicate();
        await subscriber.subscribe('cdc_events', (err) => {
            if (err) throw err;
            console.log(`[${new Date().toLocaleString()}] 📡 Subscribed to Redis channel: cdc_events`);
        });

        subscriber.on('message', async (channel, message) => {
            const start = Date.now();
            const data = JSON.parse(message);
            const { table, event, rows } = data;

            console.log(`[${new Date().toLocaleString()}] 📨 Event received: ${event.toUpperCase()} on table "${table}" (${rows.length} rows)`);

            try {
                if (event === 'writerows') {
                    for (const row of rows) {
                        const newData = sanitizeRow(row);
                        const columns = Object.keys(newData);
                        const values = Object.values(newData);

                        const nonIdColumns = columns.filter(c => c.toLowerCase() !== 'id');
                        const binds = {};

                        columns.forEach((col, i) => {
                            const val = values[i];
                            const bindName = col.toLowerCase();

                            if (val === null || val === undefined) {
                                binds[bindName] = null;
                            }
                            else if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(val)) {
                                binds[bindName] = { val, type: oracledb.DB_TYPE_TIMESTAMP };
                            }
                            else if (bindName === "gambar_base64") {
                                binds[bindName] = { val, type: oracledb.CLOB };
                            }
                            else if (typeof val === 'number') {
                                binds[bindName] = { val, type: oracledb.NUMBER };
                            }
                            else {
                                binds[bindName] = { val, type: oracledb.STRING };
                            }
                        });

                        const sql = `
                            MERGE INTO ${table} t
                            USING (SELECT ${columns.map(c => `:${c.toLowerCase()} AS ${c}`).join(', ')} FROM dual) src
                            ON (t.id = src.id)
                            WHEN MATCHED THEN UPDATE SET ${nonIdColumns.map(c => `t.${c} = src.${c}`).join(', ')}
                            WHEN NOT MATCHED THEN INSERT (${columns.join(', ')})
                            VALUES (${columns.map(c => `src.${c}`).join(', ')})
                        `;

                        console.log("SQL:", sql);
                        console.log("Binds:", binds);

                        await oracleConn.execute(sql, binds, { autoCommit: true });
                    }
                    console.log(`[${new Date().toLocaleString()}] ✅ Inserted/updated ${rows.length} row(s) into ${table}`);
                }

                else if (event === 'updaterows') {
                    for (const row of rows) {
                        const newData = sanitizeRow(row.after);
                        const columns = Object.keys(newData);
                        const values = Object.values(newData);

                        const setClause = columns.map((c, i) => `${c} = :${i + 1}`).join(', ');

                        await oracleConn.execute(
                            `UPDATE ${table} SET ${setClause} WHERE id = :id`,
                            [...values, newData.id],
                            { autoCommit: true }
                        );
                    }
                    console.log(`[${new Date().toLocaleString()}] ✅ Updated ${rows.length} row(s) in ${table}`);
                }

                else if (event === 'deleterows') {
                    for (const row of rows) {
                        await oracleConn.execute(
                            `DELETE FROM ${table} WHERE id = :id`,
                            { id: row.id },
                            { autoCommit: true }
                        );
                    }
                    console.log(`[${new Date().toLocaleString()}] 🗑️ Deleted ${rows.length} row(s) from ${table}`);
                }

            } catch (err) {
                console.error(`[${new Date().toLocaleString()}] ❌ Error processing event:`, err.message);
                console.error(`Help: https://docs.oracle.com/error-help/db/${err.message.split(':')[0]}/`);
            }

            const elapsed = ((Date.now() - start) / 1000).toFixed(3);
            console.log(`[${new Date().toLocaleString()}] ⏱️ Process completed in ${elapsed} second`);
        });

    } catch (err) {
        console.error(`❌ Oracle CDC Consumer failed:`, err);
    }
})();
