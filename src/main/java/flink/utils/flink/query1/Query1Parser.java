package flink.utils.flink.query1;

import flink.metrics.LatencyTracker;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.time.Instant;

public class Query1Parser {
    //Get only the articleID and 1 as field
    public static Tuple2<String, Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {

        return new Tuple2<>(tuple.f1,1);
    }

    public static Tuple2<String,Integer> parseMetrics(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Instant> tuple) throws IOException {
        Instant end=Instant.now();
        LatencyTracker.computeLatency(tuple.f15,end,1);
        return new Tuple2<>(tuple.f1,1);
    }
}
