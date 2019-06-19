package flink.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

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
}
