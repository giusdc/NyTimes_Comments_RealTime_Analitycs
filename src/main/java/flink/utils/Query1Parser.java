package flink.utils;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query1Parser {
    public static Tuple2<String, Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {
        return new Tuple2<String,Integer>(tuple.f1,1);
    }
}
