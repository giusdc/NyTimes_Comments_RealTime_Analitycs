package flink.metrics;

import flink.query.Query1;
import flink.query.Query2;
import flink.query.Query3;
import flink.utils.kafka.KafkaProperties;
import flink.utils.kafka.TopicDeserialization;
import kafka.TopicDeserializationMetrics;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

import static flink.utils.other.FileUtils.createFile;
import static flink.utils.other.FileUtils.createFileWithDir;

public class MainMetrics {



    public static String[] pathList={"rankhourly.csv","rankdaily.csv","rankweekly.csv","popdaily.csv","popweekly.csv","popmonthly.csv","commentdaily.csv","commentweekly.csv","commentmonthly.csv"};
    public static String[] pathMetrics={"query1latency.txt","query2latency.txt","query3latencydirect.txt","query3latencyindirect.txt"};
    volatile public static String kafkaAddress;
    volatile public static String redisAddress;

    public static void main(String[] args) throws Exception {

        kafkaAddress= args[0];
        redisAddress= args[1];

        //createFile(pathList);
        createFileWithDir("metrics",pathMetrics);
        createFileWithDir("results",pathList);
        //Set environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //Set Kafka properties
        Properties properties= KafkaProperties.getProperties();
        FlinkKafkaConsumerBase<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String,Long>> kafkasource=new FlinkKafkaConsumer011<>("comments",new TopicDeserializationMetrics(), properties).setStartFromEarliest();
        //Set source
        kafkasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String,Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String,Long> tuple16) {
                return tuple16.f5*1000;
            }
        });

        DataStream<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String,Long>> stream =env.addSource(kafkasource);


        //Query1.processMetrics(stream,redisAddress);
        //Query2.processMetrics(stream);
        Query3.processMetrics(stream,redisAddress);


        //Process Query
        env.execute();


    }


}
