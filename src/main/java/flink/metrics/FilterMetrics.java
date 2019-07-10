package flink.metrics;

import flink.utils.other.NanoClock;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class FilterMetrics implements FilterFunction<Tuple6<Long, String, String, Long, Long, Long>> {


    private String kafkaAddress;
    public FilterMetrics(String kafkaAddress) {
        this.kafkaAddress=kafkaAddress;
    }

    @Override
    public boolean filter(Tuple6<Long, String, String, Long, Long,Long> tuple6) throws Exception {
        if(tuple6.f0!=null){
            Instant now = Instant.now(new NanoClock(ZoneId.systemDefault()));
            long result = Duration.between(Instant.ofEpochMilli(0), now).toNanos();
            //System.err.println(result);
            LatencyTracker.computeLatency(tuple6.f5,result,4,this.kafkaAddress);
            return true;
        }
        return false;
    }


}
