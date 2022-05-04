package com.wasp.kafka.producer;

import com.wasp.kafka.CommonConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


@Slf4j
public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        log.info("Hi from Kafka Producer");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10; i++) {

                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(CommonConstants.TOPIC, "hello world" + i);

                //send data - async
                producer.send(producerRecord, logMetadataCallback());
            }

            //flush - synchronous - and close producer
        }
    }

    private static Callback logMetadataCallback() {
        return (metadata, exception) -> {
            //executes every time a record is successfully sent or an exception is thrown
            if (exception == null) {
                //success - no exception
                log.info("Received new metadata:\n" +
                        "topic: " + metadata.topic() + "\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp());
            } else log.error("Error while producing", exception);
        };
    }
}
