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
        //Update the rank
        if(jedis.zcard(this.key)>=1){
            Set<String> rankElements = jedis.zrange(key, 0, 2);
            HashMap<Integer, String> hashMapRank = new HashMap<>();
            rankElements.add(tupleWindows.f0+"_"+tupleWindows.f1);
            for(String rank :rankElements){
                hashMapRank.put(Integer.parseInt(rank.split("_")[1]),rank.split("_")[0]);
            }

            //Sort the hashmap with a treemap
            TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
            treeMap.putAll(hashMapRank);
            List<Integer> values=new ArrayList<>(treeMap.keySet());
            List<String> keys=new ArrayList<>(treeMap.values());
            jedis.del(this.key);

            //Give a score based on the sort
            for(int x=0;x<keys.size();x++){
                jedis.zadd(this.key,x+1,keys.get(x)+"_"+values.get(x));
                if(x==2)
                    break;
            }
        }
        //Add element if the key doesn't exist
        else{
            jedis.zadd(this.key,1,tupleWindows.f0+"_"+tupleWindows.f1);
        }
        jedis.close();
    }
}
