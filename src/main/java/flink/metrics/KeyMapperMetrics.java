package flink.metrics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.Instant;

public class KeyMapperMetrics implements FlatMapFunction<Tuple7<Long, String, String, Long, Long, Integer, Instant>, Tuple6<Long, String, String, Long, Long,Instant>> {

    private String redisAddress;
    public KeyMapperMetrics(String redisAddress) {
        this.redisAddress=redisAddress;
    }


    @Override
    public synchronized void flatMap(Tuple7<Long, String, String, Long, Long, Integer,Instant> x, Collector<Tuple6<Long, String, String, Long, Long,Instant>> collector) throws Exception {
        if (x.f1.equals("comment"))
            collector.collect(new Tuple6<>(x.f0, x.f1, x.f2, x.f3, x.f4,x.f6));
        else {
            Jedis jedis=new Jedis(redisAddress);
            if (x.f5 > 2) {
                String userId = jedis.get(String.valueOf(x.f4));
                if (userId == null)
                    collector.collect(new Tuple6<Long, String, String, Long, Long,Instant>(null, x.f1, x.f2, x.f3, x.f0,x.f6));
                else {
                    String[] id = userId.split("_");
                    collector.collect(new Tuple6<>(Long.parseLong(id[0]), x.f1, x.f2, x.f3, x.f0,x.f6));
                    String id2 = jedis.get(id[1]);
                    if (id2 == null)
                        collector.collect(new Tuple6<>(null, x.f1, x.f2, x.f3, x.f0,x.f6));
                    else
                        collector.collect(new Tuple6<>(Long.parseLong(id2.split("_")[0]), x.f1, x.f2, x.f3, x.f0,x.f6));
                }
            } else {
                String userId = jedis.get(String.valueOf(x.f4));
                if (userId == null)
                    collector.collect(new Tuple6<>(null, x.f1, x.f2, x.f3, x.f0,x.f6));
                else
                    collector.collect(new Tuple6<>(Long.parseLong(userId.split("_")[0]), x.f1, x.f2, x.f3, x.f0,x.f6));
            }
        }
    }
}
