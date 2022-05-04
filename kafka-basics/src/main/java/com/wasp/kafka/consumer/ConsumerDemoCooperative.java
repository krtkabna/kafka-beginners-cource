package com.wasp.kafka.consumer;

import com.wasp.kafka.CommonConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoCooperative {

    public static void main(String[] args) {
        log.info("Hi from Kafka Consumer");

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerUtils.MY_SECOND_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //create consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            //get a reference to the current thread
            final Thread mainThread = Thread.currentThread();

            //adding shutdown hook
            Runtime.getRuntime().addShutdownHook(ConsumerUtils.getMainThreadHook(consumer, mainThread));

            try {
                //subscribe consumer to the topic(s)
                consumer.subscribe(Collections.singletonList(CommonConstants.TOPIC));

                //poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(ConsumerUtils.logConsumerRecord());
                }
            } catch (WakeupException ignored) {
                //we ignore this as this is an expected exception when closing a consumer
                log.info("Wake up exception");
            } catch (Exception e) {
                log.error("Unexpected exception:", e);
            } finally {
                log.info("Consumer gracefully closed");
            }
        }
    }
}
