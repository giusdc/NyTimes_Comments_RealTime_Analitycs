package flink.utils.flink.query1;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;
import java.io.*;

public class Query1RankWindows extends RichMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>> implements AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow> {

    private String file="";
    private long lag;
    private transient Meter meter;


    public Query1RankWindows(String file, long lag) {
        this.file=file;
        this.lag=lag;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {

        //Get the key for searching in the db
        String id= FileUtils.getId(file)+"1"+"_"+(timeWindow.getStart()-lag);
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        writer.write(""+(timeWindow.getStart()-lag));
        new FinalRank(id,writer,3).getRank();

    }




    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_final_rank "+FileUtils.getId(this.file), new DropwizardMeterWrapper(dropwizard));
        this.meter.markEvent();
        return stringIntegerTuple2;
    }
}

