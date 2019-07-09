package flink.utils.flink.query1;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.utils.other.FileUtils;

public class Query1Rank extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {


    private String redisAddress;
    String type;
    public  Query1Rank(String type, String redisAddress) {
        this.type=type;
        this.redisAddress=redisAddress;
    }

    @Override
    public synchronized void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) {
        //Create the key from searching in the db
        String id= type+"1"+"_"+context.window().getEnd();
        Tuple2<String, Integer> tupleWindows = iterable.iterator().next();
        collector.collect(iterable.iterator().next());
        new PartialArticleRank(id,tupleWindows,redisAddress,context.window().getStart()).rank();

    }



}
