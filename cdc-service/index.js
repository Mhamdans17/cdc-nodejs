const ZongJi = require('zongji');
const Redis = require('ioredis');

const {
    MYSQL_HOST = 'localhost',
    MYSQL_USER = 'root',
    MYSQL_PASSWORD = 'root',
    MYSQL_DB = 'payment',
    REDIS_HOST = '127.0.0.1',
    REDIS_PORT = 6379,
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
});

console.log('CDC SERVICE STARTED. WAITING FOR MYSQL CHANGES...');

zongji.on('binlog', async (event) => {
    if (event.getEventName() === 'writerows' || event.getEventName() === 'updaterows' || event.getEventName() === 'deleterows') {
        console.log('BINLOG EVENT DETECTED:', event.getEventName());
        const table = event.tableMap[event.tableId].tableName;
        const rows = event.rows;
        const payload = { table, event: event.getEventName(), rows };
        await redis.publish('cdc_events', JSON.stringify(payload));
        console.log('SENT TO REDIS:', payload);
    }
});

zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    includeSchema: { [MYSQL_DB]: true },
});

process.on('SIGINT', () => {
    console.log('STOPPING CDC SERVICE');
    zongji.stop();
    process.exit();
});
