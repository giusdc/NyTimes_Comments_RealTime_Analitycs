package flink;

import flink.utils.KafkaProperties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;


import java.util.Properties;

public class MainFlink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setLatencyTrackingInterval(1000L);


        Properties properties= KafkaProperties.getProperties();

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer08<>("comments", properties));



    }
}
