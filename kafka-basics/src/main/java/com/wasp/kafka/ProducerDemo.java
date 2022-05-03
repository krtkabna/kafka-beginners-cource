package com.wasp.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemo {

    public static void main(String[] args) {
        log.info("Hi from Kafka Producer");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

            //send data - async
            producer.send(producerRecord);

            //flush - synchronous - and close
        }
    }
}
