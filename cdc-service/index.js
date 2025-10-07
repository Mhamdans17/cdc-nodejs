const ZongJi = require('zongji');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB,
    KAFKA_BROKER,
    KAFKA_TOPIC,
} = process.env;

// Setup Kafka
const kafka = new Kafka({
    clientId: 'cdc-service',
    brokers: [KAFKA_BROKER], // contoh: 'localhost:9092'
});

const producer = kafka.producer();

(async () => {
    await producer.connect();
    console.log('Kafka Producer Connected.');
})();

// Setup ZongJi
const zongji = new ZongJi({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DB,
    startAtEnd: true,
});

console.log('CDC SERVICE STARTED. WAITING FOR MYSQL CHANGES...');

zongji.on('binlog', async (event) => {
    if (['writerows', 'updaterows', 'deleterows'].includes(event.getEventName())) {
        console.log('BINLOG EVENT DETECTED:', event.getEventName());

        const table = event.tableMap[event.tableId].tableName;
        const rows = event.rows;

        const payload = { table, event: event.getEventName(), rows };

        try {
            await producer.send({
                topic: KAFKA_TOPIC || 'cdc_events',
                messages: [
                    { value: JSON.stringify(payload) }
                ],
            });
            console.log('SENT TO KAFKA:', payload);
        } catch (err) {
            console.error('KAFKA PRODUCER ERROR:', err);
        }
    }
});

zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    includeSchema: { [MYSQL_DB]: true },
});

process.on('SIGINT', async () => {
    console.log('STOPPING CDC SERVICE');
    await producer.disconnect();
    zongji.stop();
    process.exit();
});
