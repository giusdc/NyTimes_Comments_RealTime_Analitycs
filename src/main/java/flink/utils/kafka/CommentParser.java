package flink.utils.kafka;


import org.apache.flink.api.java.tuple.Tuple15;

public class CommentParser {
    public static Tuple15<Long, String, Long, Long, String,
            Long, Integer, String, Long, String, Long,String,String,Long,String> parse(String line){
        String[] comment=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        return new Tuple15<Long, String, Long, Long, String,
                Long, Integer, String, Long, String, Long,String,String,Long,String>(Long.parseLong(comment[0]),comment[1],Long.parseLong(comment[2]),Long.parseLong(comment[3]),comment[4],Long.parseLong(comment[5]),Integer.parseInt(comment[6]),comment[7],(long) Long.parseLong(comment[8]),comment[9],Long.parseLong(comment[10]),comment[11],comment[12],Long.parseLong(comment[13]),comment[14]);

    }
}
