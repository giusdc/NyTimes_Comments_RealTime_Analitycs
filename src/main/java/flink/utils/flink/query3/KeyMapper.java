package flink.utils.flink.query3;

import flink.MainFlink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class KeyMapper implements FlatMapFunction<Tuple6<Long, String, String, Long, Long, Integer>, Tuple5<Long, String, String, Long, Long>> {
    String redisAddress;
    public KeyMapper(String redisAddress) {
        this.redisAddress=redisAddress;
    }

    @Override
    public synchronized void flatMap(Tuple6<Long, String, String, Long, Long, Integer> x, Collector<Tuple5<Long, String, String, Long, Long>> collector) throws Exception {
        if (x.f1.equals("comment"))
            collector.collect(new Tuple5<>(x.f0, x.f1, x.f2, x.f3, x.f4));
        else {
            Jedis jedis=new Jedis(redisAddress);
            if (x.f5 > 2) {
                String userId = jedis.get(String.valueOf(x.f4));
                if (userId == null)
                    collector.collect(new Tuple5<Long, String, String, Long, Long>(null, x.f1, x.f2, x.f3, x.f0));
                else {
                    String[] id = userId.split("_");
                    collector.collect(new Tuple5<Long, String, String, Long, Long>(Long.parseLong(id[0]), x.f1, x.f2, x.f3, x.f0));
                    String id2 = jedis.get(id[1]);
                    if (id2 == null)
                        collector.collect(new Tuple5<Long, String, String, Long, Long>(null, x.f1, x.f2, x.f3, x.f0));
                    else
                        collector.collect(new Tuple5<Long, String, String, Long, Long>(Long.parseLong(id2.split("_")[0]), x.f1, x.f2, x.f3, x.f0));
                }
            } else {
                String userId = jedis.get(String.valueOf(x.f4));
                if (userId == null)
                    collector.collect(new Tuple5<Long, String, String, Long, Long>(null, x.f1, x.f2, x.f3, x.f0));
                else
                    collector.collect(new Tuple5<Long, String, String, Long, Long>(Long.parseLong(userId.split("_")[0]), x.f1, x.f2, x.f3, x.f0));
            }
        }
    }
}
