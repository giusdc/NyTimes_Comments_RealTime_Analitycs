package flink.query;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.*;

public class RankWindows implements AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow> {

    private String file="";
    public RankWindows(String file) {
        this.file=file;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {

        HashMap<Integer, String> hashMapRank = new HashMap<>();
        iterable.forEach(x->hashMapRank.put(x.f1,x.f0));
        TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
        treeMap.putAll(hashMapRank);
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        List<Integer> values=new ArrayList<>(treeMap.keySet());
        List<String> ids=new ArrayList<>(treeMap.values());
        writer.write(""+timeWindow.getStart()+",");
        for(int i=0;i<ids.size();i++){
            writer.write("("+ids.get(i)+","+values.get(i)+"),");
            if(i==2)
                break;
        }
        writer.write("\n");
        writer.close();

    }
}
