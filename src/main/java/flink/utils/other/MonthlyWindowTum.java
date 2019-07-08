package flink.utils.other;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MonthlyWindowTum extends WindowAssigner<Object, TimeWindow> {
    //private long timeout;
    /*
    public MonthlyWindow(long timeout) {
        this.timeout=timeout;
    }*/

    @Override
    public Collection<TimeWindow> assignWindows(Object o, long timestamp, WindowAssignerContext windowAssignerContext) {
        /*
        if (timestamp > -9223372036854775808L) {
            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                            ZoneOffset.UTC.normalized());
            LocalDateTime start = triggerTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            LocalDateTime end = triggerTime.withDayOfMonth(1).plusMonths(1).minusDays(1).withHour(23).withMinute(59).withSecond(59);

            long slide=Time.days(1).toMilliseconds();
           // long size=start.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            List<MonthlyWindow> windows = new ArrayList((int)(Time.days(30).toMilliseconds()/ slide));
           long size= timestamp - (timestamp + Time.days(30).toMilliseconds()) % Time.days(30).toMilliseconds();



            for(long startwin=size; startwin >timestamp-size; startwin -= size) {
                windows.add(new MonthlyWindow(startwin, startwin+Time.days(30).toMilliseconds()));
            }

            return windows;*/

        if (timestamp > -9223372036854775808L) {

            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                            ZoneOffset.UTC.normalized());
            LocalDateTime start = triggerTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            LocalDateTime end = triggerTime.withDayOfMonth(1).plusMonths(1).minusDays(1).withHour(23).withMinute(59).withSecond(59);

            return Collections.singletonList(new TimeWindow(start.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),end.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()));
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?");
        }


            //long start = MonthlyWindow.getWindowStartWithOffset(timestamp, 0, timeout);
            //return Collections.singletonList(new MonthlyWindow(start.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),end.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()));

    }

    private synchronized long getNumberofDays(long timestamp) {

        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                        ZoneOffset.UTC.normalized());
        return Time.days(triggerTime.toLocalDate().lengthOfMonth()).toMilliseconds();
        /*int monthvalue = triggerTime.getMonthValue();
        int yearvalue=triggerTime.getYear();
        if(monthvalue==11 ||monthvalue==4 || monthvalue==6 || monthvalue==9)
            return Time.days(30);
        if(monthvalue==2)
        {
            if((yearvalue%400==0) ||
                    (yearvalue%4==0 && yearvalue%100!=0)){
                return Time.days(29);
            }else
                return Time.days(28);

        }else
            return Time.days(31);*/
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
