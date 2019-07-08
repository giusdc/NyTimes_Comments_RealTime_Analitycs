package flink.utils.flink.query3;

import flink.MainFlink;
import flink.utils.flink.query1.FinalRank;
import flink.utils.other.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.*;

public class Query3RankWindows implements AllWindowFunction<Tuple2<Long, Float>, Object, TimeWindow> {

    private String file="";
    private String redisAddress;


    public Query3RankWindows(String file,String redisAddress) {
        this.file=file;
        this.redisAddress=redisAddress;
    }

    @Override
    public synchronized void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Float>> iterable, Collector<Object> collector) throws Exception {
        String id= FileUtils.getId(file)+"3"+"_"+(timeWindow.getEnd());
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        new FinalRank(id,writer,10, this.redisAddress).getRank();
    }
}
