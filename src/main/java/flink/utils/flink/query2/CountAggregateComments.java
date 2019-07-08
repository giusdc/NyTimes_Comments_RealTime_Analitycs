package flink.utils.flink.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CountAggregateComments implements AggregateFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> {
    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("",0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple2<String, Integer> tuple, Tuple2<String, Integer> aggr) {
        return new Tuple2<>(tuple.f0,tuple.f1+aggr.f1);
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> tuple2) {
        return new Tuple2<>(tuple2.f0,tuple2.f1);
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
        return new Tuple2<>(acc1.f0,acc1.f1+acc2.f1);
    }
}
