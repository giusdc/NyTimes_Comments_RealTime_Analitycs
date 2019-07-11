package flink.utils.flink.query1;

import flink.metrics.LatencyTracker;
import flink.utils.other.NanoClock;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class Query1Parser {
    //Get only the articleID and 1 as field
    public static Tuple2<String, Integer> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple) {

        return new Tuple2<>(tuple.f1,1);
    }

    public static Tuple2<String,Integer> parseMetrics(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> tuple,String kafkaadd) throws IOException {
        Instant now = Instant.now(new NanoClock(ZoneId.systemDefault()));
        long result = Duration.between(Instant.ofEpochMilli(0), now).toNanos();
        LatencyTracker.computeLatency(tuple.f15,result,1,kafkaadd);
        return new Tuple2<>(tuple.f1,1);
    }
}
