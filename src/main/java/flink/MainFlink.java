package flink;

import flink.query.Query1;
import flink.utils.KafkaProperties;

import flink.utils.TopicDeserialization;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;


import java.util.Properties;

public class MainFlink {
    public static void main(String[] args) throws Exception {
        //Set environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //Set Kafka properties
        Properties properties= KafkaProperties.getProperties();
        //Set source
        DataStreamSource<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream =env.addSource(new FlinkKafkaConsumer011<>("comments",new TopicDeserialization(), properties).setStartFromEarliest());
        Query1.process(stream);
        //Process Query
        env.execute();


    }
}
