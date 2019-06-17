package flink.utils;

import flink.redis.RedisConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

public class Query1Process implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    static int count=0;

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("",0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple2<String, Integer> tuple, Tuple2<String, Integer> aggr) {
        return new Tuple2<>(tuple.f0, aggr.f1 + tuple.f1);
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> tuple) {
        count++;
        RedisConfig.push(tuple,count);
        if(count==24){
            System.out.println("Fine giorno");
            count=0;
            RedisConfig.print();
        }

        return new Tuple2<>(tuple.f0,0);
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
        return new Tuple2<>(acc1.f0,acc1.f1+acc2.f1);
    }
}
