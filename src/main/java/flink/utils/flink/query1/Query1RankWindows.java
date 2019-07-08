package flink.utils.flink.query1;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;
import org.apache.kafka.common.protocol.types.Field;

import java.io.*;

public class Query1RankWindows  implements AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

    private String file="";
    private String redisAddress;


    public Query1RankWindows(String file, String redisAddress) {
        this.file=file;
        this.redisAddress=redisAddress;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

        //Get the key for searching in the db
        if(file.equals("rankdaily.csv"))
            System.out.println();
        String id= FileUtils.getId(file)+"1"+"_"+(timeWindow.getEnd());
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        //new FinalRank(id,writer,3,redisAddress).getRank();
        collector.collect(new FinalRank(id,writer,3,redisAddress).getRank());


    }
}

