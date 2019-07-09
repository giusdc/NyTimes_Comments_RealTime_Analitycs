package flink.utils.flink.query3;


import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class PartialUserRank {
    private String key;
    private Tuple2<Long,Float> tupleWindows;
    private String redisAddress;
    private long start;


    public  PartialUserRank(String key, Tuple2<Long, Float> tupleWindows, String redisAddress, long start) {
        this.key=key;
        this.tupleWindows=tupleWindows;
        this.redisAddress=redisAddress;
        this.start=start;
    }



    public synchronized void rank() {

        JedisPool pool = new JedisPool(this.redisAddress,6379);
        Jedis jedis= pool.getResource();
        //Add element with score equal to value(negative for reverse ordering)
        jedis.zadd(this.key,-1*tupleWindows.f1,tupleWindows.f0+"_"+tupleWindows.f1+"_"+start);

        //In this case remove the elements from 10th to the end for computing the rank efficiently
        if(jedis.zcard(this.key)>=11){
            jedis.zremrangeByRank(this.key,10,-1);
        }
        jedis.close();
        pool.close();
    }
}
