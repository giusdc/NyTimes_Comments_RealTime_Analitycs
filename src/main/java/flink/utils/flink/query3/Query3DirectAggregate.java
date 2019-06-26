package flink.utils.flink.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;


public class Query3DirectAggregate implements AggregateFunction<Tuple5<Long, String,String,Long,Long>, Tuple2<Long, Float>, Tuple2<Long, Float>> {

    float value=0;
    @Override
    public Tuple2<Long, Float> createAccumulator() {
        return new Tuple2<Long,Float>(0L,0f);
    }

    @Override
    public Tuple2<Long, Float> add(Tuple5<Long, String, String, Long,Long> tuple5, Tuple2<Long, Float> agg) {

        if(tuple5.f1.equals("comment")) {
                value = tuple5.f3;
                if (tuple5.f2.equals("True"))
                    value = tuple5.f3+tuple5.f3 * 0.1f;
            }

            else
                value=0;
            return new Tuple2<>(tuple5.f0,agg.f1+value);

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
