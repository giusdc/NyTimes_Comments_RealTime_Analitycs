package flink.utils.flink.query1;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.*;



public class PartialArticleRank {

    private String key;
    private Tuple2<String,Integer> tupleWindows;
    public PartialArticleRank(String key, Tuple2<String, Integer> tupleWindows) {
        this.key=key;
        this.tupleWindows=tupleWindows;
    }



    public synchronized void rank() {

        Jedis jedis=new Jedis("localhost");
        jedis.zadd(this.key,-1*tupleWindows.f1,tupleWindows.f0+"_"+tupleWindows.f1);
        if(jedis.zcard(this.key)>=4)
            jedis.zremrangeByRank(this.key,4,-1);
        jedis.close();
    }
}
