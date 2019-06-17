package flink;

import flink.utils.KafkaProperties;

import flink.utils.TopicDeserialization;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;


import java.util.Properties;

public class MainFlink {
    public static void main(String[] args) {
        //Set environment

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setLatencyTrackingInterval(1000L);
        //Set Kafka properties

        Properties properties= KafkaProperties.getProperties();
        //Set source
        DataStreamSource<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream =env.addSource(new FlinkKafkaConsumer011<>("comments",new TopicDeserialization(), properties));
        stream.writeAsText("prova");






    }
}