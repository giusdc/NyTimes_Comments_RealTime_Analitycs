package flink.utils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query3Rank extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Float>> iterable, Collector<Tuple2<Long, Float>> collector) throws Exception {
        collector.collect(((Tuple2<Long, Float>) iterable.iterator().next()));
    }
}
