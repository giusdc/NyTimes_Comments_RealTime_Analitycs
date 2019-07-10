package flink.query;

import flink.metrics.FilterMetrics;
import flink.metrics.KeyMapperMetrics;
import flink.metrics.Query3DirectAggregateMetric;
import flink.metrics.Query3IndirectAggregateMetrics;
import flink.utils.flink.query3.*;
import flink.utils.other.MonthlyWindowTum;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;

public class Query3 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream, String redisAddress) {

        //Mapper
        DataStream<Tuple6<Long, String, String, Long, Long, Integer>> mapper = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query3Parser.parse(x,redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.INT));

        //Daily direct comments'statistics
        DataStream<Tuple2<Long, Float>> dailyDirect = mapper
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3DirectAggregate());

        //Daily indirect comments'statistics
        DataStream<Tuple2<Long, Float>> dailyIndirect = mapper
                .flatMap(new KeyMapper(redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG))
                .filter(x->x.f0!=null)
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3IndirectAggregate());


        //Join daily statistics
        DataStream<Tuple2<Long, Float>> rankDaily = dailyDirect
                .join(dailyIndirect)
                .where((KeySelector<Tuple2<Long, Float>, Long>) t -> t.f0)
                .equalTo((KeySelector<Tuple2<Long, Float>, Long>) t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new JoinValues())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new Query3Rank("D",redisAddress))
                .setParallelism(3);

        //Weekly statistics
        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("W",redisAddress))
                .setParallelism(3);

        //Monthly statistics
        DataStream<Tuple2<Long, Float>> rankMonthly = rankDaily
                .keyBy(0)
                .window(new MonthlyWindowTum())
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("M",redisAddress))
                .setParallelism(3);

        //Get results
        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("D",redisAddress))
                .writeAsText("popdaily");
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("W",redisAddress))
                .writeAsText("popweekly");
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("M",redisAddress))
                .writeAsText("popmonthly");

    }

    public static void processMetrics(DataStream<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Instant>> stream, String redisAddress) {

        //Mapper
        DataStream<Tuple7<Long, String, String, Long, Long, Integer,Instant>> mapper = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query3Parser.parseMetrics(x,redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.INT,Types.INSTANT));

        //Daily direct comments'statistics
        DataStream<Tuple2<Long, Float>> dailyDirect = mapper
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3DirectAggregateMetric());




        //Daily indirect comments'statistics
        DataStream<Tuple2<Long, Float>> dailyIndirect = mapper
                .flatMap(new KeyMapperMetrics(redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.INSTANT))
                .filter(new FilterMetrics())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3IndirectAggregateMetrics());


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
                .process(new Query3Rank("D",redisAddress))
                .setParallelism(3);

        //Weekly statistics
        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("W",redisAddress))
                .setParallelism(3);

        //Monthly statistics
        DataStream<Tuple2<Long, Float>> rankMonthly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(30),Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("M",redisAddress))
                .setParallelism(3);

        //Get results
        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("D",redisAddress))
                .writeAsText("popdaily");
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("W",redisAddress))
                .writeAsText("popweekly");
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("M",redisAddress))
                .writeAsText("popmonthly");



    }
}
