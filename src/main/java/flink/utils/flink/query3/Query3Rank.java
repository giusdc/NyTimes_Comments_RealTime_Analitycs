package flink.utils.flink.query3;

import flink.utils.other.FileUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query3Rank extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple, TimeWindow> {

    private String file;
    private String redisAddress;

    public  Query3Rank(String file,String redisAddress) {
        this.file=file;
        this.redisAddress=redisAddress;
    }


    @Override
    public synchronized void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Float>> iterable, Collector<Tuple2<Long, Float>> collector) throws Exception {
        if(file.equals("popdaily.csv")){
            System.out.println();
        }
        Tuple2<Long, Float> tupleWindows = iterable.iterator().next();
        String id= FileUtils.getId(file)+"3"+"_"+context.window().getEnd();
        collector.collect(((Tuple2<Long, Float>) iterable.iterator().next()));
        new PartialUserRank(id,tupleWindows,this.redisAddress,context.window().getStart()).rank();
    }
}
