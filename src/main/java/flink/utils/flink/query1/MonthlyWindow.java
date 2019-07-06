package flink.utils.flink.query1;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MonthlyWindow extends WindowAssigner<Object, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
    //private long timeout;
    /*
    public MonthlyWindow(long timeout) {
        this.timeout=timeout;
    }*/

    @Override
    public Collection<org.apache.flink.streaming.api.windowing.windows.TimeWindow> assignWindows(Object o, long timestamp, WindowAssignerContext windowAssignerContext) {
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

        if (timestamp <= -9223372036854775808L) {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?");
        } else {

            /*Tuple3<String,Integer,Long> tuple3=(Tuple3<String,Integer,Long>)o;
            long timestamp2=tuple3.f2*1000;
            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp2),
                            ZoneOffset.UTC.normalized());
            LocalDateTime startDate = triggerTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
            long size= Time.days(startDate.toLocalDate().lengthOfMonth()).toMilliseconds();*/
            long size=getNumberofDays(timestamp);
            long slide=Time.days(1).toMilliseconds();
            List<org.apache.flink.streaming.api.windowing.windows.TimeWindow> windows = new ArrayList((int)(size/slide));
            //long timestamp3=startDate.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            long lastStart = timestamp- (timestamp + slide) % slide;

            for(long start = lastStart; start > timestamp - size; start -= slide) {
                windows.add(new org.apache.flink.streaming.api.windowing.windows.TimeWindow(start, start + size));
            }

            return windows;
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
    public Trigger<Object, org.apache.flink.streaming.api.windowing.windows.TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<org.apache.flink.streaming.api.windowing.windows.TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
