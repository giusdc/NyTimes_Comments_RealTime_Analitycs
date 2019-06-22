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
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){

        //Hour statistic
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv"));

        //Daily statistic
      DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .timeWindow(Time.days(1), Time.hours(1))
                         .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv"));

       //Week statistic
        DataStream<Tuple2<String, Integer>> rankWeek = rankDaily
                .keyBy(0)
                .timeWindow(Time.days(7), Time.days(1))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankweekly.csv"));

        //Rank Calculator

       rank1h.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankhourly.csv",3600000-1));
       rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankdaily.csv",86400000-1));
        rankWeek.timeWindowAll(Time.milliseconds(1)).apply(
                new Query1RankWindows("rankweekly.csv",604800000-1));
    }
}
