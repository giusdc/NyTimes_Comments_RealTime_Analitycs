package flink.utils.flink.query1;

import flink.redis.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FinalRank implements Runnable {

    private String key;
    private BufferedWriter writer;
    private int position;
    public FinalRank(String key, BufferedWriter writer,int position) {
        this.key=key;
        this.writer=writer;
        this.position=position;
    }

    @Override
    public void run()  {
        synchronized (this){
            Jedis jedis=new Jedis("localhost");
            List<String> finalRank = jedis.sort(this.key, new SortingParams().alpha().desc());
            try {
                writer.write(""+finalRank.get(0).split("_")[1]+",");

                for(int i=0;i<finalRank.size();i++){
                    writer.write("("+finalRank.get(i).split("_")[0]+","+finalRank.get(i).split("_")[2]+"),");
                    if(i==position-1)
                        break;
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
