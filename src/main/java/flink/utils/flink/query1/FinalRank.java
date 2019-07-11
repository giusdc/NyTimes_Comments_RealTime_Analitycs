package flink.utils.flink.query1;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Set;

public class FinalRank {

    private String key;
    private int position;
    private String redisAddress;

    public FinalRank(String key, int position, String redisAddress) {
        this.key = key;
        this.position = position;
        this.redisAddress = redisAddress;
    }


    public synchronized String getRank() {

        //Get position element from Redis
        JedisPool pool = new JedisPool(this.redisAddress, 6379);
        Jedis jedis = pool.getResource();
        Set<String> rank = jedis.zrange(key, 0, position - 1);
        String[] finalRank = rank.toArray(new String[0]);
        String result = "";
        //Get result
        String triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(finalRank[0].split("_")[2])),
                        ZoneOffset.UTC.normalized()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
        result += triggerTime + " [";
        for (int i = 0; i < finalRank.length; i++) {
            result += "(" + finalRank[i].split("_")[0] + "," + finalRank[i].split("_")[1] + "),";
            if (i == position - 1){
                result += "(" + finalRank[i].split("_")[0] + "," + finalRank[i].split("_")[1] + ")";
                break;

            }
        }
        result+="]";
        jedis.del(this.key);
        jedis.close();
        pool.close();
        return result;


    }
}

