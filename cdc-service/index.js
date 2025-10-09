const ZongJi = require('zongji');
const Redis = require('ioredis');
require('dotenv').config();
const chalk = require('chalk');

const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB,
    REDIS_HOST,
    REDIS_PORT,
} = process.env;

const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
});

const zongji = new ZongJi({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DB,
    startAtEnd: true,
});

function timestamp() {
    return new Date().toISOString().replace('T', ' ').split('.')[0];
}

console.log(chalk.cyan(`[${timestamp()}] CDC SERVICE STARTED — Listening for MySQL changes...`));

zongji.on('binlog', async (event) => {
    const eventName = event.getEventName();
    if (['writerows', 'updaterows', 'deleterows'].includes(eventName)) {
        const table = event.tableMap[event.tableId].tableName;
        const rows = event.rows;
        const payload = { table, event: eventName, rows };

        await redis.publish('cdc_events', JSON.stringify(payload));

        // stringify rows jadi satu baris
        let rowsText = JSON.stringify(rows);
        if (rowsText.length > 500) {
            rowsText = rowsText.substring(0, 500) + '... (truncated)';
        }

        console.log(
            `[${timestamp()}] EVENT: ${eventName.toUpperCase()} | TABLE: ${table} | ROWS: ${rowsText}`
        );
    }
});

zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    includeSchema: { [MYSQL_DB]: true },
});

process.on('SIGINT', () => {
    console.log(`[${timestamp()}] STOPPING CDC SERVICE`);
    zongji.stop();
    process.exit();
});
