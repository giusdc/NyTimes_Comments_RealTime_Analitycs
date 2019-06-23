package flink.utils.flink.query3;

import flink.utils.flink.query1.PushRank;
import flink.utils.other.FileUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query3Rank extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple, TimeWindow> {

    String file;
    public  Query3Rank(String file) {
        this.file=file;
    }


    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Float>> iterable, Collector<Tuple2<Long, Float>> collector) throws Exception {
        Tuple2<Long, Float> tupleWindows = iterable.iterator().next();
        String id= FileUtils.getId(file)+"3"+context.window().getStart();
        collector.collect(((Tuple2<Long, Float>) iterable.iterator().next()));
        Thread t=new Thread(new PushUserRank(id,tupleWindows));
        t.start();
    }
}
