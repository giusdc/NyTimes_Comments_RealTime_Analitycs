package flink.utils.flink.query3;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinValues implements JoinFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Tuple2<Long, Float>> {

    float wa = 0.3f;
    float wb = 0.7f;

    @Override
    public synchronized Tuple2<Long, Float>  join(Tuple2<Long, Float> tupleDirect, Tuple2<Long, Float> tupleIndirect) {
        if(tupleDirect.f0==2073520)
            System.out.println();
        return new Tuple2<>(tupleDirect.f0, wa * tupleDirect.f1 + wb * tupleIndirect.f1);
    }
}

