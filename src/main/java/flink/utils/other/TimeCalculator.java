package flink.utils.other;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeCalculator {

    public static int computeOffset(long timeWindowStart){
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeWindowStart),
                        ZoneOffset.UTC.normalized());
        LocalDateTime uctDate =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(0),
                        ZoneOffset.UTC.normalized());

        int day1=triggerTime.toLocalDate().getDayOfWeek().ordinal()+1;
        int day2 = uctDate.toLocalDate().getDayOfWeek().ordinal()+1;

        return day1-day2;

    }
}
