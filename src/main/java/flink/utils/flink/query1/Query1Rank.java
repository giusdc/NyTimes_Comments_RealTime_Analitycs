package flink.utils.flink.query1;

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
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) {

        //Create the key from searching in the db

        String id= FileUtils.getId(file)+"1"+"_"+context.window().getStart();
        Tuple2<String, Integer> tupleWindows = iterable.iterator().next();
        collector.collect(iterable.iterator().next());
        new PartialArticleRank(id,tupleWindows).rank();

    }



}
