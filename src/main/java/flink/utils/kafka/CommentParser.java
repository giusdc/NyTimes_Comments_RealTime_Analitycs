package flink.utils.kafka;

import flink.utils.other.NanoClock;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class CommentParser {
    public static Tuple15<Long, String, Long, Long, String,
            Long, Integer, String, Long, String, Long,String,String,Long,String> parse(String line){


        String[] comment=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if(checkLine(comment)){
            return new Tuple15<>(Long.parseLong(comment[0]),comment[1],Long.parseLong(comment[2]),Long.parseLong(comment[3]),comment[4],Long.parseLong(comment[5]),Integer.parseInt(comment[6]),comment[7],(long) Long.parseLong(comment[8]),comment[9],Long.parseLong(comment[10]),comment[11],comment[12],Long.parseLong(comment[13]),comment[14]);
        }
        else
            return new Tuple15<>(-1L,null,-1L,-1L,null,-1L,-1,null,-1L,null,-1L,null,null,-1L,null);
    }


    private static boolean checkLine(String[] str) {
        //check non valid value
        try {
            if (Long.parseLong(str[0]) < 1514764800)
                return false;
            if (Integer.parseInt(str[2]) < 0)
                return false;
            if (Integer.parseInt(str[3]) < 0)
                return false;
            if (!(str[4].equals("comment") || str[4].equals("userReply")))
                return false;
            if (Long.parseLong(str[5]) < 1514764800)
                return false;
            if (!(Integer.parseInt(str[6]) >= 1 && Integer.parseInt(str[6]) <= 3))
                return false;
            if (!(str[7].equals("False") || str[7].equals("True")))
                return false;
            if (str[4].equals("comment")) {
                if (!(str[8].equals("0") && str[9].equals("")))
                    return false;

            }else
            {
                if(Integer.parseInt(str[8])<0)
                    return false;
            }
            if (Integer.parseInt(str[10]) < 0)
                return false;
            if(Integer.parseInt(str[13])<0)
                return false;
        } catch (NumberFormatException num) {
            return false;
        }
        return true;

    }

    public static Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> parseMetrics(String line) {
        String[] comment=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if(checkLine(comment)){
            Instant now = Instant.now(new NanoClock(ZoneId.systemDefault()));
            long result = Duration.between(Instant.ofEpochMilli(0), now).toNanos();
            return new Tuple16<>(Long.parseLong(comment[0]),comment[1],Long.parseLong(comment[2]),Long.parseLong(comment[3]),comment[4],Long.parseLong(comment[5]),Integer.parseInt(comment[6]),comment[7],(long) Long.parseLong(comment[8]),comment[9],Long.parseLong(comment[10]),comment[11],comment[12],Long.parseLong(comment[13]),comment[14],result);
        }
        else
            return new Tuple16<>(-1L,null,-1L,-1L,null,-1L,-1,null,-1L,null,-1L,null,null,-1L,null,-1L);
    }
}
