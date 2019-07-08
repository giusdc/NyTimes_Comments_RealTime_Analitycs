package flink.query;

import flink.metrics.FilterMetrics;
import flink.metrics.KeyMapperMetrics;
import flink.metrics.Query3DirectAggregateMetric;
import flink.metrics.Query3IndirectAggregateMetrics;
import flink.utils.flink.query3.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
                .process(new Query3Rank("popdaily.csv",redisAddress));

        //Weekly statistics
        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popweekly.csv",redisAddress));

        //Monthly statistics
        DataStream<Tuple2<Long, Float>> rankMonthly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(30),Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popmonthly.csv",redisAddress));

        //Get results
        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popdaily.csv",86400000-1,redisAddress));
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popweekly.csv",604800000-1,redisAddress));
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popmonthly.csv",2592000000L-1,redisAddress));

    }

    public static void processMetrics(DataStream<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long>> stream, String redisAddress) {

        //Mapper
        DataStream<Tuple7<Long, String, String, Long, Long, Integer,Long>> mapper = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query3Parser.parseMetrics(x,redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.INT,Types.LONG));

        //Daily direct comments'statistics
        DataStream<Tuple2<Long, Float>> dailyDirect = mapper
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query3DirectAggregateMetric());




        //Daily indirect comments'statistics
        DataStream<Tuple2<Long, Float>> dailyIndirect = mapper
                .flatMap(new KeyMapperMetrics(redisAddress))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG,Types.LONG))
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
                .process(new Query3Rank("popdaily.csv",redisAddress));

        //Weekly statistics
        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popweekly.csv",redisAddress));

        //Monthly statistics
        DataStream<Tuple2<Long, Float>> rankMonthly = rankDaily
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(30),Time.days(1)))
                .aggregate(new Query3AggregateIntermediate(), new Query3Rank("popmonthly.csv",redisAddress));

        //Get results
        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popdaily.csv",86400000-1,redisAddress));
        rankWeekly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popweekly.csv",604800000-1,redisAddress));
        rankMonthly.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("popmonthly.csv",2592000000L-1,redisAddress));



    }
}
