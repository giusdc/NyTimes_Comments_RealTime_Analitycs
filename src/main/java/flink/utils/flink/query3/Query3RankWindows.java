package flink.utils.flink.query3;

import flink.utils.flink.query1.FinalRank;
import flink.utils.other.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.*;

public class Query3RankWindows implements AllWindowFunction<Tuple2<Long, Float>, Object, TimeWindow> {

    private String file="";
    private long lag;


    public Query3RankWindows(String file, long lag) {
        this.file=file;
        this.lag=lag;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Float>> iterable, Collector<Object> collector) throws Exception {



        String id= FileUtils.getId(file)+"3"+(timeWindow.getStart()-lag);
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        Thread t=new Thread(new FinalRank(id,writer,10));
        t.start();
    }
}
