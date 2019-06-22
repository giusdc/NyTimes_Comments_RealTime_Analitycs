package flink.utils.flink.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query3AggregateIntermediate implements AggregateFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple2<Long, Float>> {

    @Override
    public Tuple2<Long, Float> createAccumulator() {
        return new Tuple2<Long,Float>(0L,0f);
    }

    @Override
    public Tuple2<Long, Float> add(Tuple2<Long,Float> tuple2, Tuple2<Long, Float> agg) {
            return new Tuple2<>(tuple2.f0,agg.f1+tuple2.f1);
    }

    @Override
    public Tuple2<Long, Float> getResult(Tuple2<Long, Float> tuple2) {

        return new Tuple2<>(tuple2.f0,tuple2.f1);

    }

    @Override
    public Tuple2<Long, Float> merge(Tuple2<Long, Float> agg1, Tuple2<Long, Float> agg2) {
        return new Tuple2<>(agg1.f0,agg1.f1+agg2.f1);
    }
}