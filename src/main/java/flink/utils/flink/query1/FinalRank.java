package flink.utils.flink.query1;

import flink.MainFlink;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
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
        this.redisAddress = redisAddress;
    }


    public synchronized String getRank() {

        //Get position element from Redis
        JedisPool pool = new JedisPool(this.redisAddress, 6379);
        //Jedis jedis=new Jedis(MainFlink.redisAddress);
        Jedis jedis = pool.getResource();
        Set<String> rank = jedis.zrange(key, 0, position - 1);
        String[] finalRank = rank.toArray(new String[0]);
        String result = "";
        //Write result on file
        String triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(finalRank[0].split("_")[2])),
                        ZoneOffset.UTC.normalized()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
        result += triggerTime + ",";
        for (int i = 0; i < finalRank.length; i++) {
            result += "(" + finalRank[i].split("_")[0] + "," + finalRank[i].split("_")[1] + "),";
            if (i == position - 1)
                break;
        }
        jedis.del(this.key);
        jedis.close();
        pool.close();
        return result;


    }
}

