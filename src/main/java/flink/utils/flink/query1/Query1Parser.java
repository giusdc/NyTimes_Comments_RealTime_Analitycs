package flink.utils.flink.query1;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query1Parser {
    //Get only the articleID and 1 as field
    public static Tuple2<String, Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {

        return new Tuple2<>(tuple.f1,1);
    }
}
