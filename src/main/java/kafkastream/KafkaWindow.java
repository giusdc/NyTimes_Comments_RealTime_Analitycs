package kafkastream;

import org.apache.flink.api.common.time.Time;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KafkaWindow extends Windows<Window> {

    @Override
    public Map<Long, Window> windowsFor(long timestamp) {

        long advanceMs=Time.days(1).toMilliseconds();


        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                        ZoneOffset.UTC.normalized());
        LocalDateTime startDate = triggerTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
        long timestamp2=startDate.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        int days= startDate.toLocalDate().lengthOfMonth();
        long size= org.apache.flink.streaming.api.windowing.time.Time.days(days).toMilliseconds();
        //long size=getNumberofDays(timestamp);

        LocalDateTime endDate;









        //long windowStart = Math.max(0L, timestamp - size + advanceMs) / advanceMs* advanceMs;

        LinkedHashMap windows;
        long start = timestamp2;

        for(windows = new LinkedHashMap();windows.size()<days ;) {
            endDate=startDate.plusMonths(1).minusDays(1).withHour(23).withMinute(59).withSecond(59);
            long end=endDate.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            TimeWindow window = new TimeWindow(start,end);
            windows.put(start, window);
            startDate=startDate.minusDays(1);
            start=startDate.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        }

        return windows;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long gracePeriodMs() {
        return -1L;
    }
    private synchronized long getNumberofDays(long timestamp) {

        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                        ZoneOffset.UTC.normalized());
        return Time.days(triggerTime.toLocalDate().lengthOfMonth()).toMilliseconds();

    }
}
