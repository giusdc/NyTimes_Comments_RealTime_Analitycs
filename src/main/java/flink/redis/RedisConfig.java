package flink.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisConfig {
    public static Jedis jedis;

    public static void connect(){
        jedis = new Jedis("localhost");
        System.out.println("Connection to server sucessfully");
        //check whether server is running or not
        System.out.println("Server is running: "+jedis.ping());
    }
     public static void push(Tuple2<String,Integer> tuple,int count){
        jedis.hset(tuple.f0,String.valueOf(count),String.valueOf(tuple.f1));
         System.out.println();
     }

     public static void print(){
         List<String> list = jedis.lrange("*", 0 ,23);
         System.out.println(list.size());

         for(int i = 0; i<list.size(); i++) {
             System.out.println("Stored string in redis:: "+list.get(i));
         }
     }

    public static void pushRank1(String key, Tuple2<String, Integer> tupleWindows) {

        System.out.println();
        //There are elements in the rank
        if(jedis.zcard(key)>=1){
            Set<String> rankElements = jedis.zrange(key, 0, -1);
            HashMap<Integer, String> hashMapRank = new HashMap<>();
            rankElements.add(tupleWindows.f0+"_"+key.substring(1)+"_"+tupleWindows.f1);
            for(String rank :rankElements){
                hashMapRank.put(Integer.parseInt(rank.split("_")[2]),rank.split("_")[0]+"_"+rank.split("_")[1]);
            }
            TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
            treeMap.putAll(hashMapRank);
            List<Integer> values=new ArrayList<>(treeMap.keySet());
            List<String> keys=new ArrayList<>(treeMap.values());
            jedis.del(key);
            for(int x=0;x<keys.size();x++){
                jedis.zadd(key,x+1,keys.get(x)+"_"+values.get(x));
                if(x==2)
                    break;
            }
        }
        else{
            jedis.zadd(key,1,tupleWindows.f0+"_"+key.substring(1)+"_"+tupleWindows.f1);
        }


    }

    public static Set<String> getRank(String key) {
        return jedis.zrange(key,0,2);
    }
}
