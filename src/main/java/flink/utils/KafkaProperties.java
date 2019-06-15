package flink.utils;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getProperties(){
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        //props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-consumer-group");
        return props;
    }
}
