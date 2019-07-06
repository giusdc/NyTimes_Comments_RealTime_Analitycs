package flink.utils.flink.query3;

import flink.MainFlink;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;

import java.util.*;

public class PartialUserRank {
    private String key;
    private Tuple2<Long,Float> tupleWindows;
    private String redisAddress;


    public  PartialUserRank(String key, Tuple2<Long, Float> tupleWindows,String redisAddress) {
        this.key=key;
        this.tupleWindows=tupleWindows;
        this.redisAddress=redisAddress;
    }



    public synchronized void rank() {

        Jedis jedis=new Jedis(this.redisAddress);
        //Add element with score equal to value(negative for reverse ordering)
        jedis.zadd(this.key,-1*tupleWindows.f1,tupleWindows.f0+"_"+tupleWindows.f1);

        //In this case remove the elements from 10th to the end for computing the rank efficiently
        if(jedis.zcard(this.key)>=11){
           // Set<String> rankElements = jedis.zrange(key, 0, 9);
            //HashMap<Float, String> hashMapRank = new HashMap<>();
            //rankElements.add(tupleWindows.f0+"_"+tupleWindows.f1);
            /*
            for(String rank :rankElements){
                hashMapRank.put(Float.parseFloat(rank.split("_")[1]),rank.split("_")[0]);
            }
            TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
            treeMap.putAll(hashMapRank);
            List<Float> values=new ArrayList<>(treeMap.keySet());
            List<String> keys=new ArrayList<>(treeMap.values());*/
            jedis.zremrangeByRank(this.key,10,-1);
            //jedis.zadd(this.key,tupleWindows.f1,tupleWindows.f0+"_"+tupleWindows.f1);
            /*jedis.del(this.key);
            for(int x=0;x<keys.size();x++){
                jedis.zadd(this.key,x+1,keys.get(x)+"_"+values.get(x));
                if(x==9)
                    break;
            }*/
        }
        jedis.close();
    }
}
