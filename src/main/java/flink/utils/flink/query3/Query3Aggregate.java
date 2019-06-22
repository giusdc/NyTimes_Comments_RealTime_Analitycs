package flink.utils.flink.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import scala.Int;

public class Query3Aggregate implements AggregateFunction<Tuple4<Long, String,String,Long>, Tuple3<Long, Float,Integer>, Tuple2<Long, Float>> {

    private float wa=0.3f;
    private float wb= 0.7f;
    float value=0;
    @Override
    public Tuple3<Long, Float, Integer> createAccumulator() {
        return new Tuple3<Long,Float, Integer>(0L,0f,0);
    }

    @Override
    public Tuple3<Long, Float, Integer> add(Tuple4<Long, String, String, Long> tuple4, Tuple3<Long, Float, Integer> agg) {
        //(tuple.f0, aggr.f1 + tuple.f1);
        if(tuple4.f1.equals("comment")){
            value=tuple4.f3;
            if(tuple4.f2.equals("True"))
                 value= tuple4.f3 * 0.1f;
            return new Tuple3<>(tuple4.f0,agg.f1+value,agg.f2);
        }
        else
            return new Tuple3<>(tuple4.f0,agg.f1,agg.f2+1);
    }

    @Override
    public Tuple2<Long, Float> getResult(Tuple3<Long, Float, Integer> tuple3) {

        return new Tuple2<Long,Float>(tuple3.f0,wa*tuple3.f1+wb*tuple3.f2);

    }

    @Override
    public Tuple3<Long, Float, Integer> merge(Tuple3<Long, Float, Integer> agg1, Tuple3<Long, Float, Integer> agg2) {
        return new Tuple3<>(agg1.f0,agg1.f1+agg2.f1,agg1.f2+agg2.f2);
    }
}
