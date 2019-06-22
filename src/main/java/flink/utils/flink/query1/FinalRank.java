package flink.utils.flink.query1;

import flink.redis.RedisConfig;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public class FinalRank implements Runnable {

    private String key;
    private BufferedWriter writer;
    public FinalRank(String key, BufferedWriter writer) {
        this.key=key;
        this.writer=writer;
    }

    @Override
    public void run()  {
        synchronized (this){
            Jedis jedis=new Jedis("localhost");
            Set<String> rank = jedis.zrange(this.key,0,2);
            String[] finalRank = rank.toArray(new String[0]);
            try {
                writer.write(""+finalRank[0].split("_")[1]+",");

                for(int i=0;i<finalRank.length;i++){
                    writer.write("("+finalRank[i].split("_")[0]+","+finalRank[i].split("_")[2]+"),");
                }
                writer.write("\n");
                jedis.del(this.key);
                writer.close();
                jedis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
