package com.wasp.kafka.consumer;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerConstants {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC = "demo_topic";
    static final String GROUP_ID = "my-second-consumer-group";
}
