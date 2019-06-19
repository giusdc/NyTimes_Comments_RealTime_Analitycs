package flink.query;

import flink.utils.Query1Parser;
import flink.utils.Query1Process;
import flink.utils.Query1Rank;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

public class Query1 {
    //In seconds
    public static final int WINDOWS_LENGTH=3600;
    public static final int SLIDING_WINDOWS_LENGHT=3600;
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){
        int count=0;
        DataStream<Tuple2<String, Integer>> sum = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.of(WINDOWS_LENGTH, TimeUnit.MILLISECONDS))
                .aggregate(new Query1Process(),new Query1Rank());
        sum.timeWindowAll(Time.of(WINDOWS_LENGTH,TimeUnit.MILLISECONDS)).apply(
                new AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                        System.out.println();
                        System.out.println();
                    }
                }
        );






        sum.print();


    }
}
