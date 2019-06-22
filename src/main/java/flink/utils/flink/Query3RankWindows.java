package flink.utils.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

public class Query3RankWindows implements AllWindowFunction<Tuple2<Long, Float>, Object, TimeWindow> {

    String file="";
    public Query3RankWindows(String file) {
        this.file=file;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Float>> iterable, Collector<Object> collector) throws Exception {
        HashMap<Float, Long> hashMapRank = new HashMap<>();
        iterable.forEach(z->hashMapRank.put(z.f1,z.f0));
        TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
        treeMap.putAll(hashMapRank);
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        List<Float> values=new ArrayList<>(treeMap.keySet());
        List<Long> ids=new ArrayList<>(treeMap.values());
        writer.write(""+timeWindow.getStart()+",");
        for(int i=0;i<ids.size();i++){
            writer.write("("+ids.get(i)+","+values.get(i)+"),");
            if(i==10)
                break;
        }
        writer.write("\n");
        writer.close();
    }
}
