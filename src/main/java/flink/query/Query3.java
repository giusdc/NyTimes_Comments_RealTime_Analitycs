package flink.query;

import flink.utils.flink.query3.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query3 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        //Mapper
        DataStream<Tuple6<Long, String, String, Long, Long, Integer>> mapper = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query3Parser.parse(x))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.INT));

        //Daily direct comments'statistics
        DataStream<Tuple2<Long, Float>> dailyDirect = mapper
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3DirectAggregate());




        //Daily indirect comments'statistics
        DataStream<Tuple2<Long, Float>> dailyIndirect = mapper
                .flatMap(new KeyMapper())
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG))
                .filter(x->x.f0!=null)
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3IndirectAggregate());


        dailyDirect.print();
        dailyIndirect.print();

        //Join daily statistics
        DataStream<Tuple2<Long, Float>> rankDaily = dailyDirect
                .join(dailyIndirect)
                .where((KeySelector<Tuple2<Long, Float>, Long>) t -> t.f0)
                .equalTo((KeySelector<Tuple2<Long, Float>, Long>) t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new JoinValues())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new Query3Rank("popdaily.csv"));

        //Weekly statistics
        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popweekly.csv"));

        //Monthly statistics
        DataStream<Tuple2<Long, Float>> rankMonthly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(30),Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popmonthly.csv"));

        //Get results
        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popdaily.csv",86400000-1));
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popweekly.csv",604800000-1));
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popmonthly.csv",2592000000L-1));

    }
}
