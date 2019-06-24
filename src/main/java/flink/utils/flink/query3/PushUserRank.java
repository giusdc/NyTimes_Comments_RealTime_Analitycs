package flink.utils.flink.query3;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;

import java.util.*;

public class PushUserRank{
    private String key;
    private Tuple2<Long,Float> tupleWindows;
    public PushUserRank(String key, Tuple2<Long, Float> tupleWindows) {
        this.key=key;
        this.tupleWindows=tupleWindows;
    }

    //@Override
    //TODO A volte ci sono score uguali,sarà colpa dei thread?Si :(
    public void run() {
        synchronized (this)
        {
            Jedis jedis=new Jedis("localhost");
            if(jedis.zcard(this.key)>=1){
                Set<String> rankElements = jedis.zrange(key, 0, 9);
                HashMap<Float, String> hashMapRank = new HashMap<>();
                rankElements.add(tupleWindows.f1+"_"+tupleWindows.f0+"_"+key.substring(2));
                for(String rank :rankElements){
                    hashMapRank.put(Float.parseFloat(rank.split("_")[0]),rank.split("_")[1]+"_"+rank.split("_")[2]);
                }
                TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
                treeMap.putAll(hashMapRank);
                List<Float> values=new ArrayList<>(treeMap.keySet());
                List<String> keys=new ArrayList<>(treeMap.values());
                jedis.del(this.key);
                for(int x=0;x<keys.size();x++){
                    jedis.zadd(this.key,x+1,values.get(x)+"_"+keys.get(x));
                    if(x==9)
                        break;
                }
            }
            else{
                jedis.zadd(this.key,1,tupleWindows.f1+"_"+tupleWindows.f0+"_"+key.substring(2));
            }
            jedis.close();
        }
    }

    public synchronized void rank() {

        Jedis jedis=new Jedis("localhost");
        if(jedis.zcard(this.key)>=1){
            Set<String> rankElements = jedis.zrange(key, 0, 9);
            HashMap<Float, String> hashMapRank = new HashMap<>();
            rankElements.add(tupleWindows.f0+"_"+tupleWindows.f1);
            for(String rank :rankElements){
                hashMapRank.put(Float.parseFloat(rank.split("_")[1]),rank.split("_")[0]);
            }
            TreeMap treeMap = new TreeMap<>(Collections.reverseOrder());
            treeMap.putAll(hashMapRank);
            List<Float> values=new ArrayList<>(treeMap.keySet());
            List<String> keys=new ArrayList<>(treeMap.values());
            jedis.del(this.key);
            for(int x=0;x<keys.size();x++){
                jedis.zadd(this.key,x+1,keys.get(x)+"_"+values.get(x));
                if(x==9)
                    break;
            }
        }
        else{
            jedis.zadd(this.key,1,tupleWindows.f0+"_"+tupleWindows.f1);
        }
        jedis.close();
    }
}
