package flink.metrics;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.Instant;

public class Query3IndirectAggregateMetrics implements AggregateFunction<Tuple6<Long, String,String,Long,Long, Instant>, Tuple2<Long, Float>, Tuple2<Long, Float>> {

    private float value=0;
    @Override
    public Tuple2<Long, Float> createAccumulator() {
        return new Tuple2<Long,Float>(0L,0f);
    }

    @Override
    public Tuple2<Long, Float> add(Tuple6<Long, String, String, Long,Long,Instant> tuple5, Tuple2<Long, Float> agg) {

        if(tuple5.f1.equals("comment")) {
            value = 0;
        }
        else {
            value = 1;
        }
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
