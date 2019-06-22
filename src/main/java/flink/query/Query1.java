package flink.query;

import flink.utils.flink.query1.Query1Parser;
import flink.utils.flink.query1.Query1Aggregate;
import flink.utils.flink.query1.Query1Rank;
import flink.utils.flink.Query1RankWindows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;




public class Query1 {
    //Windows length in seconds
    public static final long WINDOW_HOURLY=3600;
    public static final long WINDOW_DAILY=86400;
    public static final long WINDOW_WEEK=604800;
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){

        //Hour statistic
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_HOURLY))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv"));

        //Daily statistic
      DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .timeWindow(Time.milliseconds(WINDOW_DAILY), Time.milliseconds(WINDOW_HOURLY))
                         .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv"));

       //Week statistic
        DataStream<Tuple2<String, Integer>> rankWeek = rankDaily
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_WEEK), Time.milliseconds(WINDOW_DAILY))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankweekly.csv"));

        //Rank Calculator
       rank1h.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankhourly.csv",WINDOW_HOURLY-1));
       rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankdaily.csv",WINDOW_DAILY-1));
        rankWeek.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankweekly.csv",WINDOW_WEEK-1));
    }
}
