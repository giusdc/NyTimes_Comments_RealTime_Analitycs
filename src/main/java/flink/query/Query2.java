package flink.query;

import flink.utils.flink.query1.Query1Aggregate;
import flink.utils.flink.query2.Query2Parser;
import flink.utils.flink.query2.Query2Result;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Query2 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        //Windows every 2 hours
        DataStream<Tuple2<String, Integer>> countHours = stream
                .map(x -> Query2Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
                .filter(x -> x.f1.equals("comment"))
                .map(x -> Query2Parser.removeCommentType(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .sum(1);



        DataStream<Tuple2<String, Integer>> countWeekly=countHours
                .keyBy(0)
                .timeWindow(Time.days(7))
                .sum(1);

        DataStream<Tuple2<String, Integer>> countMonthly=countWeekly
                .keyBy(0)
                .timeWindow(Time.days(30))
                .sum(1);

        countHours
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new Query2Result("commentdaily.csv",0));

        countWeekly
                .timeWindowAll(Time.milliseconds(1))
                .apply(new Query2Result("commentweekly.csv",604800000-1));

        countMonthly
                .timeWindowAll(Time.milliseconds(1))
                .apply(new Query2Result("commentmonthly.csv",2592000000L-1));

        countHours.print();


    }
}