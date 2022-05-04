package com.wasp.kafka.consumer;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.function.Consumer;

@Slf4j
@UtilityClass
public class ConsumerUtils {
    final String MY_SECOND_CONSUMER_GROUP = "my-second-consumer-group";

    Thread getMainThreadHook(KafkaConsumer<String, String> consumer, Thread mainThread) {
        return new Thread(() -> {
            log.info("Shutdown detected, exiting by calling consumer.wakeup()...");
            consumer.wakeup();

            //join the main thread to allow code execution in it
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.warn("Thread interrupted!", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    Consumer<ConsumerRecord<String, String>> logConsumerRecord() {
        return consumerRecord -> {
            log.info("key: " + consumerRecord.key() + ", value: " + consumerRecord.value());
            log.info("partition: " + consumerRecord.partition() + ", offset: " + consumerRecord.offset());
        };
    }
}
