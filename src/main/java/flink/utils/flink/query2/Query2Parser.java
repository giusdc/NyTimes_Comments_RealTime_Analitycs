package flink.utils.flink.query2;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class Query2Parser {

    //Get only the articleID and 1 as field

    public static Tuple3<String,String,Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tuple.f5*1000),
                        ZoneOffset.UTC.normalized());
        String key=getKey(triggerTime.getHour());
        return new Tuple3<>(key,tuple.f4,1);
    }

    private static String getKey(int hour) {

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

    public static Tuple2<String,Integer> removeCommentType(Tuple3<String, String, Integer> x) {
        return new Tuple2<>(x.f0,x.f2);
    }
}
