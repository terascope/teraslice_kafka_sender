'use strict';

const Promise = require('bluebird');

function newProcessor(context, opConfig) {
    let producerReady = false;

    const producer = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        options: {
            type: 'producer'
        },
        rdkafka_options: {
            'compression.codec': opConfig.compression
        }
    }).client;

    producer.on('ready', () => {
        producerReady = true;
    });

    return data => new Promise(((resolve, reject) => {
        function error(err) {
            reject(err);
        }

        function process() {
            if (producerReady) {
                data.forEach((record) => {
                    let key = null;
                    let timestamp = null;

                    if (opConfig.id_field) {
                        // TODO: make sure the field really exists
                        key = record[opConfig.id_field];
                    }

                    if (opConfig.timestamp_field) {
                        // TODO: make sure the field really contains a date
                        timestamp = new Date(record[opConfig.timestamp_field]).getTime();
                    } else if (opConfig.timestamp_now) {
                        timestamp = Date.now();
                    }

                    producer.produce(
                        opConfig.topic,
                        // This is the partition. There may be use cases where
                        // we'll need to control this.
                        null,
                        new Buffer(JSON.stringify(record)),
                        key,
                        timestamp
                    );
                });

                // TODO: this flush timeout may need to be configurable
                producer.flush(30000, (err) => {
                    // Remove the error listener so they don't accrue across slices.
                    producer.removeListener('event.error', error);

                    if (err) {
                        return reject(err);
                    }

                    return resolve(data);
                });
            } else {
                setTimeout(process, 100);
            }
        }

        producer.on('event.error', error);

        process();
    }));
}


function schema() {
    return {
        topic: {
            doc: 'Name of the Kafka topic to send data to',
            default: '',
            format: 'required_String'
        },
        id_field: {
            doc: 'Field in the incoming record that contains keys',
            default: '',
            format: String
        },
        timestamp_field: {
            doc: 'Field in the incoming record that contains a timestamp to set on the record',
            default: '',
            format: String
        },
        timestamp_now: {
            doc: 'Set to true to have a timestamp generated as records are added to the topic',
            default: '',
            format: String
        },
        connection: {
            doc: 'The Kafka producer connection to use.',
            default: 'default',
            format: String
        },
        compression: {
            doc: 'Type of compression to use',
            default: 'gzip',
            format: ['none', 'gzip', 'snappy', 'lz4']
        }
    };
}

module.exports = {
    newProcessor,
    schema
};
