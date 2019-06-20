package flink.utils;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple4;

public class Query2Parser {
    public static Tuple4<Long, String,String,Long> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> x) {
        return new Tuple4<Long,String, String, Long>(x.f13,x.f4,x.f7,x.f10);
    }
}
