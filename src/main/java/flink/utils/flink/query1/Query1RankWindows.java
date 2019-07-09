package flink.utils.flink.query1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Query1RankWindows  implements AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

    private String type;
    private String redisAddress;


    public Query1RankWindows(String type, String redisAddress) {
        this.type=type;
        this.redisAddress=redisAddress;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

        //Get the key for searching in the db
        String id= type+"1"+"_"+(timeWindow.getEnd());
        collector.collect(new FinalRank(id,3,redisAddress).getRank());


    }
}

