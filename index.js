'use strict';

var Promise = require("bluebird");

function newProcessor(context, opConfig) {
    var producer_ready = false;

    var producer = context.foundation.getConnection({
        type: "kafka",
        endpoint: opConfig.connection
    }).client;

    producer.on('ready', function() {
        producer_ready = true;
    });

    return function(data) {
        return new Promise(function(resolve, reject) {
            function error(err) {
                reject(err);
            }

            function process() {
                if (producer_ready) {
                    data.forEach(function(record) {
                        var key = null;
                        var timestamp = null;

                        if (opConfig.id_field) {
                            // TODO: make sure the field really exists
                            key = record[opConfig.id_field];
                        }

                        if (opConfig.timestamp_field) {
                            // TODO: make sure the field really contains a date
                            timestamp = new Date(record[opConfig.timestamp_field]).getTime();
                        }
                        else if (opConfig.timestamp_now) {
                            timestamp = Date.now();
                        }

                        producer.produce(
                            opConfig.topic,
                            null, // This is the partition. There may be use cases where we'll need to control this.
                            new Buffer(JSON.stringify(record)),
                            key,
                            timestamp
                        );
                    });

                    // TODO: this flush timeout may need to be configurable
                    producer.flush(30000, function(err) {
                        // Remove the error listener so they don't accrue across slices.
                        producer.removeListener('event.error', error);

                        if (err) {
                            return reject(err);
                        }

                        resolve(data);
                    });
                }
                else {
                    setTimeout(process, 100);
                }
            }

            producer.on('event.error', error);

            process();
        });
    }
}

function schema(){
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
            default: '',
            format: 'required_String'
        }
    }
}

module.exports = {
    newProcessor: newProcessor,
    schema: schema
};