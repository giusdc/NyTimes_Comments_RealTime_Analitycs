package flink.utils.flink.query2;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Query2Result implements AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow> {

    private String file;

    public Query2Result(String file) {
        this.file=file;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {

        if(file.equals("commentmonthly.csv"))
            System.out.println();
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        String triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeWindow.getStart()),
                        ZoneOffset.UTC.normalized()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));

        //triggerTime.format(formatter);
        writer.write(triggerTime+",");
        List<Tuple2<String,Integer>> list= Lists.newArrayList(iterable.iterator());
        HashMap<String,Integer> hashMapValues=new HashMap<>();
        for (Tuple2<String, Integer> tuple2 : list) {
            hashMapValues.put(tuple2.f0, tuple2.f1);
        }
        String[] key={"count_h00","count_h02","count_h04","count_h06","count_h08","count_h10","count_h12","count_h14","count_h16","count_h18","count_h20","count_h22"};
        //Check if there is a value for the hourly slot, in this case write the value otherwise write 0
        for (String s : key) {
            writer.write("" + hashMapValues.getOrDefault(s, 0) + "," + "");
        }
        writer.write("\n");
        /*
        //Sorting for having from count_h00 to count_h22
        list.sort(Comparator.comparing(x -> x.f0));

        for (int x=0;x<list.size();x++){
            writer.write(""+list.get(x).f1+","+"");
        }*/

        writer.close();
    }
}
