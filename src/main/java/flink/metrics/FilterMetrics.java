package flink.metrics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.Instant;

public class FilterMetrics implements FilterFunction<Tuple6<Long, String, String, Long, Long, Instant>> {




    @Override
    public boolean filter(Tuple6<Long, String, String, Long, Long,Instant> tuple6) throws Exception {
        if(tuple6.f0!=null){
            LatencyTracker.computeLatency(tuple6.f5,Instant.now(),4);
            return true;
        }
        return false;
    }


}
