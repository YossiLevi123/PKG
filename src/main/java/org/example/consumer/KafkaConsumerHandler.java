package org.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.data.DataAbs;
import org.example.data.DataSerdes;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerHandler {

    private static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String GROUP_ID = "kafka.group.id";
    private static final String INPUT_TOPIC = "kafka.input.topic";

    public static Consumer<String, DataAbs> createConsumer(Properties properties) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty(GROUP_ID));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DataSerdes.class.getName());


        // Create the consumer using props.
        final Consumer<String, DataAbs> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(properties.getProperty(INPUT_TOPIC)));

        return consumer;
    }
}
