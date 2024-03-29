package flink;

import flink.query.Query1;
import flink.query.Query3;
import flink.query.Query2;
import flink.utils.kafka.KafkaProperties;

import flink.utils.kafka.TopicDeserialization;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;


import java.util.Properties;

public class MainFlink {

    volatile public static String kafkaAddress;
    volatile public static String redisAddress;

    public static void main(String[] args) throws Exception {

        kafkaAddress= args[0];
        redisAddress= args[1];

        //Set environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //Set Kafka properties
        Properties properties= KafkaProperties.getProperties(kafkaAddress);
        //Set source
        FlinkKafkaConsumerBase<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> kafkasource=new FlinkKafkaConsumer011<>("comments",new TopicDeserialization(), properties).setStartFromEarliest();
        //Assign timestamp
        kafkasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15) {
                return tuple15.f5*1000;
            }
        });

        DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream =env.addSource(kafkasource);

        //Queries processing
        Query1.process(stream,redisAddress);
        Query2.process(stream);
        Query3.process(stream,redisAddress);

        //Execute queries
        env.execute();


    }
}
