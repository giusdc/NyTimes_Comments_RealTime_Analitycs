package flink.utils.flink.query2;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.List;

public class CountProcessComments extends ProcessWindowFunction<Tuple2<String,Integer>, Tuple3<String,Integer,Long>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, Integer, Long>> collector) throws Exception {
        List<Tuple2<String,Integer>> list=new ArrayList<>();
        List<Tuple3<String,Integer,Long>> listfinal=new ArrayList<>();
        iterable.forEach(list::add);
        for(int x=0;x<list.size();x++){
            listfinal.add(new Tuple3<>(list.get(x).f0,list.get(x).f1,context.window().getStart()));
            collector.collect(listfinal.get(x));
        }
    }
}
