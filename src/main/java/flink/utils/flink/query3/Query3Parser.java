package flink.utils.flink.query3;

import flink.MainFlink;
import flink.metrics.LatencyTracker;
import org.apache.flink.api.java.tuple.*;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class Query3Parser {
    public  synchronized static Tuple6<Long, String,String,Long,Long,Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> x, String redisAddress) {

        Jedis jedis=new Jedis(redisAddress);
        //Delete a tuple after two weeks
        jedis.setex(String.valueOf(x.f3),2679/*31 giorni*/,String.valueOf(x.f13)+"_"+x.f8);
        jedis.close();
        if(x.f13==2073520)
            System.out.println();

        return new Tuple6<Long,String, String, Long,Long,Integer>(x.f13,x.f4,x.f7,x.f10,x.f8,x.f6);
    }

    public synchronized static Tuple5<Long, String,String,Long,Long> changeKey(Tuple5<Long, String, String, Long, Long> x) {

        if(x.f1.equals("comment"))
            return new Tuple5<>(x.f0,x.f1,x.f2,x.f3,x.f4);
        else {

            Jedis jedis=new Jedis(MainFlink.redisAddress);
            //Case when the comment id doesn't exist
            if(jedis.get(String.valueOf(x.f4))==null)
                return new Tuple5<Long, String, String, Long, Long>(null, x.f1, x.f2, x.f3, x.f0);
            long userId = Long.parseLong(jedis.get(String.valueOf(x.f4)));
            return new Tuple5<Long, String, String, Long, Long>(userId, x.f1, x.f2, x.f3, x.f0);
        }

    }

    public static Tuple7<Long, String,String,Long,Long,Integer,Long> parseMetrics(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> x, String redisAddress) throws IOException {
        Jedis jedis=new Jedis(redisAddress);
        //Delete a tuple after two weeks
        jedis.setex(String.valueOf(x.f3),2679/*31 giorni*/,String.valueOf(x.f13)+"_"+x.f8);
        jedis.close();
        LatencyTracker.computeLatency(x.f15,System.nanoTime(),3);
        return new Tuple7<Long,String, String, Long,Long,Integer,Long>(x.f13,x.f4,x.f7,x.f10,x.f8,x.f6,x.f15);
    }
}
