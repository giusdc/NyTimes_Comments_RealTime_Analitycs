package flink.utils.flink.query1;

import flink.MainFlink;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class FinalRank {

    private String key;
    private BufferedWriter writer;
    private int position;
    private String redisAddress;

    public FinalRank(String key, BufferedWriter writer, int position, String redisAddress) {
        this.key = key;
        this.writer = writer;
        this.position = position;
        this.redisAddress=redisAddress;
    }


    public synchronized void getRank() {

        //Get position element from Redis
        JedisPool pool = new JedisPool(this.redisAddress, 6379);
        //Jedis jedis=new Jedis(MainFlink.redisAddress);
        Jedis jedis = pool.getResource();
        Set<String> rank = jedis.zrange(key, 0, 9);
        String[] finalRank = rank.toArray(new String[0]);

        //Write result on file
        try {
            writer.write("" + key.split("_")[1] + ",");

            for (int i = 0; i < finalRank.length; i++) {
                writer.write("(" + finalRank[i].split("_")[0] + "," + finalRank[i].split("_")[1] + "),");
                if (i == position - 1)
                    break;
            }
            writer.write("\n");
            jedis.del(this.key);
            writer.close();
            jedis.close();
            pool.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

