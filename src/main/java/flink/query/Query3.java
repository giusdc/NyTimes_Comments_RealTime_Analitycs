package flink.query;

import flink.utils.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;



public class Query3 {


    public static final int WINDOW_DAILY=86400;
    public static final int WINDOW_WEEK=604800;
    public static final int WINDOW_MONTH=2592000;
    public static final int SLIDING_WINDOW_LENGHT=WINDOW_DAILY;
    public static final int SLIDING_WINDOW_WEEK_LENGHT=WINDOW_WEEK;

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        DataStream<Tuple2<Long, Float>> rankDaily = stream
                .map(x -> Query2Parser.parse(x))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_DAILY))
                .aggregate(new Query3Process(), new Query3Rank());

        DataStream<Tuple2<Long, Float>> rankWeekly = rankDaily
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_WEEK), Time.milliseconds(WINDOW_DAILY))
                .aggregate(new Query3ProcessIntermediate(), new Query3Rank());

        DataStream<Tuple2<Long, Float>> rankMonthly = rankWeekly
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOW_MONTH), Time.milliseconds(WINDOW_WEEK))
                .aggregate(new Query3ProcessIntermediate(), new Query3Rank());

        rankDaily.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("pophourly.csv"));

    }
}
