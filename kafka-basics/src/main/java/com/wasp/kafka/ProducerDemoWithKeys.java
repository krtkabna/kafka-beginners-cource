package com.wasp.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithKeys {

    public static void main(String[] args) {
        log.info("Hi from Kafka Producer");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10; i++) {

                //create producer record
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data - async
                producer.send(producerRecord, logMetadataCallback(producerRecord.key()));

                sleepASecond();
            }

            //flush - synchronous - and close producer
        }
    }

    private static void sleepASecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.warn("Thread interrupted!", e);
            Thread.currentThread().interrupt();
        }
    }

    private static Callback logMetadataCallback(String key) {
        return (metadata, exception) -> {
            //executes every time a record is successfully sent or an exception is thrown
            if (exception == null) {
                //success - no exception
                log.info("Received new metadata:\n" +
                        "topic: " + metadata.topic() + "\n" +
                        "key: " + key + "\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp());
            } else log.error("Error while producing", exception);
        };
    }

}
