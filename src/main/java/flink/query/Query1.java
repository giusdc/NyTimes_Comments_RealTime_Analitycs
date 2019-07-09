package flink.query;

import flink.utils.flink.query1.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class Query1 {
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream, String redisAddress){


        //Hour statistics
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("H",redisAddress))
                .setParallelism(3);

        //Daily statistics
        DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .window(TumblingEventTimeWindows.of(Time.days(1)))
                         .aggregate(new Query1Aggregate(), new Query1Rank("D",redisAddress))
                         .setParallelism(3);

       //Week statistics
        DataStream<Tuple2<String, Integer>> rankWeek = rank1h
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                .aggregate(new Query1Aggregate(), new Query1Rank("W",redisAddress))
                .setParallelism(3);

        //Getting rank
       rank1h
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("H",redisAddress))
                .writeAsText("rankhourly", FileSystem.WriteMode.NO_OVERWRITE);

       rankDaily
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("D",redisAddress))
               .writeAsText("rankdaily", FileSystem.WriteMode.NO_OVERWRITE);
        rankWeek
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                new Query1RankWindows("W",redisAddress))
                .writeAsText("rankweekly", FileSystem.WriteMode.NO_OVERWRITE);
    }

    public static void processMetrics(DataStream<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long>> stream, String redisAddress) {

        //Hour statistics
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query1Parser.parseMetrics(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("H",redisAddress))
                .setParallelism(3);

        //Daily statistics
        DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("D",redisAddress))
                .setParallelism(3);

        //Week statistics
        DataStream<Tuple2<String, Integer>> rankWeek = rank1h
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Query1Aggregate(), new Query1Rank("W",redisAddress))
                .setParallelism(3);

        //Getting rank
        rank1h
                .timeWindowAll(Time.milliseconds(1))
                .apply(new Query1RankWindows("H",redisAddress))
                .writeAsText("rankhourly", FileSystem.WriteMode.NO_OVERWRITE);
        rankDaily
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                        new Query1RankWindows("D",redisAddress))
                .writeAsText("rankdaily", FileSystem.WriteMode.NO_OVERWRITE);
        rankWeek
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                        new Query1RankWindows("W",redisAddress))
                .writeAsText("rankweekly", FileSystem.WriteMode.NO_OVERWRITE);
    }
}




