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

public class Query3RankWindows implements AllWindowFunction<Tuple2<Long, Float>, String, TimeWindow> {

    private String type;
    private String redisAddress;


    public Query3RankWindows(String type,String redisAddress) {
        this.type=type;
        this.redisAddress=redisAddress;
    }

    @Override
    public synchronized void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Float>> iterable, Collector<String> collector) throws Exception {
        if(type.equals("M"))
            System.out.println();
        String id= type+"3"+"_"+(timeWindow.getEnd());
        collector.collect(new FinalRank(id,10, this.redisAddress).getRank());

    }
}
