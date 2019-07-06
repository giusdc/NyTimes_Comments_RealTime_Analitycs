package flink.query;

import flink.utils.flink.query1.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;


public class Query1 {
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream, String redisAddress){


        //Hour statistics
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv",redisAddress));

        //Daily statistics
        DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .window(SlidingEventTimeWindows.of(Time.days(1),Time.hours(1)))
                         .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv",redisAddress));

       //Week statistics
        DataStream<Tuple2<String, Integer>> rankWeek = rank1h
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankweekly.csv",redisAddress));

        //Getting rank
       rank1h
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("rankhourly.csv",3600000-1,redisAddress));
       rankDaily
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("rankdaily.csv",86400000-1,redisAddress));
        rankWeek
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                new Query1RankWindows("rankweekly.csv",604800000-1,redisAddress));
    }

    public static void processMetrics(DataStream<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long>> stream, String redisAddress) {

        //Hour statistics
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query1Parser.parseMetrics(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv",redisAddress));

        //Daily statistics
        DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(1),Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv",redisAddress));

        //Week statistics
        DataStream<Tuple2<String, Integer>> rankWeek = rank1h
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankweekly.csv",redisAddress));

        //Getting rank
        rank1h
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                        new Query1RankWindows("rankhourly.csv",3600000-1,redisAddress));
        rankDaily
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                        new Query1RankWindows("rankdaily.csv",86400000-1,redisAddress));
        rankWeek
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                        new Query1RankWindows("rankweekly.csv",604800000-1,redisAddress));
    }
}




