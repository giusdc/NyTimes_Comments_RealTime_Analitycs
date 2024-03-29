package flink.utils.kafka;

import kafkastream.kafkaoperators.MyEventTimeExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getProperties(String kafkaAddress){
        Properties props = new Properties();

        props.put("bootstrap.servers", kafkaAddress+":9092");
        props.put("group.id", "test-consumer-group");
        return props;
    }

    public static Properties createStreamProperties(String address) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "kafka-stream");
        props.put(StreamsConfig.CLIENT_ID_CONFIG,
                "kafka-stream-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                address+":9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG,"DEBUG");
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG,"2000");

        return props;
    }
}
