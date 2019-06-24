package flink.utils.flink.query1;

import flink.redis.RedisConfig;
import flink.utils.flink.query1.FinalRank;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;

import java.io.*;
import java.util.*;

public class Query1RankWindows implements AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow> {

    private String file="";
    private long lag;


    public Query1RankWindows(String file, long lag) {
        this.file=file;
        this.lag=lag;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {

        //TODO Scrittura non nel thread,scrive random, ultima ora perchè non viene scritta?
        String id= FileUtils.getId(file)+"1"+"_"+(timeWindow.getStart()-lag);
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        new FinalRank(id,writer,3).getRank();

    }



}

