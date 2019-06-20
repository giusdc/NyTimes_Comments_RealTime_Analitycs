package flink;

import flink.query.Query1;
import flink.query.Query3;
import flink.redis.RedisConfig;
import flink.utils.KafkaProperties;

import flink.utils.TopicDeserialization;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import redis.clients.jedis.Jedis;


import javax.annotation.Nullable;
import java.io.File;
import java.util.Properties;

import static flink.utils.FileUtils.createFile;

public class MainFlink {

    public static String[] pathList={"rankhourly.csv","rankdaily.csv","rankweekly.csv","pophourly.csv"};
    public static final int WINDOWS_LENGTH=3600;
    public static final int WINDOWS_DAILY=7200; //CHange
    public static final int WINDOWS_WEEK=604800;
    public static final int SLIDING_WINDOWS_LENGHT=3600;
    public static final int SLIDING_WINDOWS_DAILY_LENGHT=86400;
    public static void main(String[] args) throws Exception {
        createFile(pathList);

        //Connect to Redis
        RedisConfig.connect();
        //Set environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Set Kafka propertiesrank1h.print();
        Properties properties= KafkaProperties.getProperties();

        FlinkKafkaConsumerBase<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> kafkasource=new FlinkKafkaConsumer011<>("comments",new TopicDeserialization(), properties).setStartFromLatest();
        //Set source
/*
kafkasource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>>(Time.seconds(0)) {

            @Override
            public long extractTimestamp(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15) {
                System.out.println();
                return tuple15.f5;
            }
        });*/


        kafkasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15) {
                return tuple15.f5;
            }
        });

        DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream =env.addSource(kafkasource);


        Query1.process(stream);
        //Query3.process(stream);

        //Process Query
        env.execute();


    }
}
