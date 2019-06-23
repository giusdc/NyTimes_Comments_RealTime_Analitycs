package flink.utils.flink.query1;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.*;



public class PushRank implements Runnable {

    private String key;
    private Tuple2<String,Integer> tupleWindows;
    public PushRank(String key, Tuple2<String, Integer> tupleWindows) {
        this.key=key;
        this.tupleWindows=tupleWindows;
    }

    @Override
    public void run() {
        synchronized (this)
        {
            Jedis jedis=new Jedis("localhost");
            if(jedis.zcard(this.key)>=1){
                Set<String> rankElements = jedis.zrange(key, 0, 2);
                HashMap<Integer, String> hashMapRank = new HashMap<>();
                rankElements.add(tupleWindows.f1+"_"+tupleWindows.f0+"_"+key.substring(2));
                for(String rank :rankElements){
                    hashMapRank.put(Integer.parseInt(rank.split("_")[0]),rank.split("_")[1]+"_"+rank.split("_")[2]);
                }
                TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
                treeMap.putAll(hashMapRank);
                List<Integer> values=new ArrayList<>(treeMap.keySet());
                List<String> keys=new ArrayList<>(treeMap.values());
                jedis.del(this.key);
                for(int x=0;x<keys.size();x++){
                    jedis.zadd(this.key,x+1,values.get(x)+"_"+keys.get(x));
                    if(x==2)
                        break;
                }
            }
            else{
                jedis.zadd(this.key,1,tupleWindows.f1+"_"+tupleWindows.f0+"_"+key.substring(2));
            }
            jedis.close();
        }

    }
}
