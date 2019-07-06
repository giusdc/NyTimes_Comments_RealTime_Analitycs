package flink.utils.flink.query2;

import flink.metrics.LatencyTracker;
import org.apache.flink.api.java.tuple.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class Query2Parser {


    public static Tuple3<String,String,Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f5*1000),
                        ZoneOffset.UTC.normalized());
        //Get the key based on the hours of the tuple
        String key=getKey(triggerTime.getHour());
        return new Tuple3<>(key,tuple.f4,1);
    }

    public static String getKey(int hour) {

        String key="count_h";
        if(hour%2==0){
            if(hour<=9)
                key+="0"+hour;
            else
                key+=hour;
        }else{
            if(hour<=9)
                key+="0"+(hour-1);
            else
                key+=hour-1;
        }
        return key;
    }

    //Remove the field related to the comment type
    public static Tuple2<String,Integer> removeCommentType(Tuple3<String, String, Integer> x) {
        return new Tuple2<>(x.f0,x.f2);
    }


    public static Tuple4<String,String,Integer,Long> parseMetrics(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> tuple) {
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f5*1000),
                        ZoneOffset.UTC.normalized());
        //Get the key based on the hours of the tuple
        String key=getKey(triggerTime.getHour());
        return new Tuple4<>(key,tuple.f4,1,tuple.f15);
    }

    public static Tuple2<String,Integer> removeCommentTypeMetrics(Tuple4<String, String, Integer, Long> x) throws IOException {
        LatencyTracker.computeLatency(x.f3,System.nanoTime(),2);
        return new Tuple2<>(x.f0,x.f2);
    }
}
