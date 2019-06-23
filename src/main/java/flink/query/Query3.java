package flink.query;

import flink.utils.flink.query3.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigInteger;


public class Query3 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        DataStream<Tuple2<Long, Float>> rankDaily = stream
                .map(x -> Query3Parser.parse(x))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .aggregate(new Query3Aggregate(), new Query3Rank("popdaily.csv"));

        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .timeWindow(Time.days(1), Time.hours(1))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popweekly.csv"));

        DataStream<Tuple2<Long, Float>> rankMonthly = rankWeekly
                .keyBy(0)
                .timeWindow(Time.days(30), Time.days(7))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popmonthly.csv"));

        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popdaily.csv",3600000-1));
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popweekly.csv",86400000-1));
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popmonthly.csv",259200000-1));

    }
}
