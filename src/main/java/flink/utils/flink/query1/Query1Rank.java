package flink.utils.flink.query1;

import flink.redis.RedisConfig;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;

public class Query1Rank extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

    String file;
    public  Query1Rank(String file) {
        this.file=file;
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {


        String id= FileUtils.getId(file)+context.window().getStart();
        Tuple2<String, Integer> tupleWindows = iterable.iterator().next();
        RedisConfig.pushRank1(id,tupleWindows);
        collector.collect(((Tuple2<String, Integer>) iterable.iterator().next()));


    }

    //Get an id on the basis of file

}
