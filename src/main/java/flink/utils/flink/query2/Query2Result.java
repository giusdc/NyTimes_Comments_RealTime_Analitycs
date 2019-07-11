package flink.utils.flink.query2;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.HashMap;
import java.util.List;

public class Query2Result implements AllWindowFunction<Tuple3<String, Integer,Long>, String, TimeWindow> {



    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<String, Integer,Long>> iterable, Collector<String> collector){
        List<Tuple3<String,Integer,Long>> list= Lists.newArrayList(iterable.iterator());
        String result;
        String triggerTime =  Instant.ofEpochMilli(list.get(0).f2).atZone(ZoneId.of("UTC")).toLocalDateTime().format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));

        result=triggerTime+" [";

        HashMap<String,Integer> hashMapValues=new HashMap<>();
        for (Tuple3<String, Integer,Long> tuple2 : list) {
            hashMapValues.put(tuple2.f0, tuple2.f1);
        }
        String[] key={"count_h00","count_h02","count_h04","count_h06","count_h08","count_h10","count_h12","count_h14","count_h16","count_h18","count_h20","count_h22"};
        //Check if there is a value for the hourly slot, in this case write the value otherwise write 0

        /*
        for (String s : key) {
            result+=hashMapValues.getOrDefault(s, 0) + ",";
        }
         */

        for (int i = 0; i <key.length ; i++) {
            result+=hashMapValues.getOrDefault(key[i], 0) + ",";
            if(i==key.length-1)
                result+=hashMapValues.getOrDefault(key[i], 0) + "]";

        }
        collector.collect(result);
    }
}
