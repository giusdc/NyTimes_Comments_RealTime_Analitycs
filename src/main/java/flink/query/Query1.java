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

import static flink.MainFlink.*;


public class Query1 {
    //In seconds

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){
        //Hour statistic
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOWS_LENGTH))
                .aggregate(new Query1Process(), new Query1Rank());
        //Daily statistic
      DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .timeWindow(Time.milliseconds(WINDOWS_DAILY), Time.milliseconds(SLIDING_WINDOWS_LENGHT))
                         .aggregate(new Query1Process(), new Query1Rank());

       //Week statistic
        DataStream<Tuple2<String, Integer>> rankWeek = rankDaily
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOWS_WEEK), Time.milliseconds(SLIDING_WINDOWS_DAILY_LENGHT))
                .aggregate(new Query1Process(), new Query1Rank());

        rank1h.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankhourly.csv"));
       rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankdaily.csv"));
        rankWeek.timeWindowAll(Time.milliseconds(1)).apply(
                new RankWindows("rankweekly.csv"));
    }
}
