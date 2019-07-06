package flink.utils.kafka;

import flink.MainFlink;
import kafkastream.MyEventTimeExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getProperties(){
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost"+":9092");
        props.put("group.id", "test-consumer-group");
        return props;
    }

    public static Properties createStreamProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "map-function-lambda-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG,
                "map-function-lambda-example-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                MainFlink.kafkaAddress+":9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return props;
    }
}
