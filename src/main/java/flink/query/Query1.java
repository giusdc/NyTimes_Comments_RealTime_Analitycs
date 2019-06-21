package flink.query;

import flink.utils.Query1Parser;
import flink.utils.Query1Process;
import flink.utils.Query1Rank;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;




public class Query1 {
    //In seconds
    public static final int WINDOW_HOURLY=3600;
    public static final int WINDOW_DAILY=86400;
    public static final int WINDOW_WEEK=604800;
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){
        //Hour statistic
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_HOURLY))
                .aggregate(new Query1Process(), new Query1Rank());
        //Daily statistic
      DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .timeWindow(Time.milliseconds(WINDOW_DAILY), Time.milliseconds(WINDOW_HOURLY))
                         .aggregate(new Query1Process(), new Query1Rank());

       //Week statistic
        DataStream<Tuple2<String, Integer>> rankWeek = rankDaily
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_WEEK), Time.milliseconds(WINDOW_DAILY))
                .aggregate(new Query1Process(), new Query1Rank());

        //Rank Calculator
        rank1h.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankhourly.csv"));
       rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankdaily.csv"));
        rankWeek.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankweekly.csv"));
    }
}
