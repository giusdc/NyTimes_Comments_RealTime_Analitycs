package flink.utils.flink;

import flink.redis.RedisConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
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


        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        String id= FileUtils.getId(file)+(timeWindow.getStart()-lag);
        Set<String> rank = RedisConfig.getRank(id);
        writer.write(""+rank.iterator().next().split("_")[1]+",");
        for(int i=0;i<rank.size();i++){
            writer.write("("+rank.iterator().next().split("_")[0]+","+rank.iterator().next().split("_")[2]+"),");
        }
        writer.write("\n");
        //RedisConfig.jedis.del(id);
        writer.close();
        System.out.println();

    }
}
