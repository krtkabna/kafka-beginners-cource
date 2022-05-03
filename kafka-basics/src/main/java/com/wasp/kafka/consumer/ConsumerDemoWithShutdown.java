package com.wasp.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

import static com.wasp.kafka.consumer.ConsumerConstants.*;

@Slf4j
public class ConsumerDemoWithShutdown {


    public static void main(String[] args) {
        log.info("Hi from Kafka Consumer");

        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            //get a reference to a current thread
            final Thread mainThread = Thread.currentThread();

            //adding shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown detected, exiting by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow code execution in it
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.warn("Thread interrupted!", e);
                    Thread.currentThread().interrupt();
                }
            }));

            try {
                //subscribe consumer to the topic
                consumer.subscribe(Arrays.asList(TOPIC));

                //poll for new data
                while (true) {
                    log.info("polling");
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(logConsumerRecord());
                }
            } catch (WakeupException ignored) {
                log.info("Wake up exception");
            } catch (Exception e) {
                log.error("Unexpected exception:", e);
            } finally {
                log.info("Consumer gracefully closed");
            }
        }
    }

    private static Consumer<ConsumerRecord<String, String>> logConsumerRecord() {
        return consumerRecord -> {
            log.info("key: " + consumerRecord.key() + ", value: " + consumerRecord.value());
            log.info("partition: " + consumerRecord.partition() + ", offset: " + consumerRecord.offset());
        };
    }
}
