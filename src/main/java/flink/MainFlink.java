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

import static flink.utils.other.FileUtils.createFile;

public class MainFlink {

    public static String[] pathList={"rankhourly.csv","rankdaily.csv","rankweekly.csv","popdaily.csv","popweekly.csv","popmonthly.csv","commentdaily.csv","commentweekly.csv","commentmonthly.csv","query1latency.txt","query2latency.txt","query3latency.txt"};
    volatile public static String kafkaAddress;
    volatile public static String redisAddress;

    public static void main(String[] args) throws Exception {

        kafkaAddress= args[0];
        redisAddress= args[1];

        createFile(pathList);
        //Set environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //Set Kafka properties
        Properties properties= KafkaProperties.getProperties();
        FlinkKafkaConsumerBase<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> kafkasource=new FlinkKafkaConsumer011<>("comments",new TopicDeserialization(), properties).setStartFromEarliest();
        //Set source
       kafkasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15) {
                return tuple15.f5*1000;
            }
        });



        DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream =env.addSource(kafkasource);


       //Query1.process(stream,redisAddress);
       Query2.process(stream);
       //Query3.process(stream,redisAddress);


        //Process Query
        env.execute();


    }
}
