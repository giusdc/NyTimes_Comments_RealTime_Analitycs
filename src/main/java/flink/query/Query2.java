package flink.query;
import flink.utils.flink.query2.Query2Parser;
import flink.utils.flink.query2.Query2Result;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class Query2 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        //Compute count each 2 hours
        DataStream<Tuple2<String, Integer>> countHours = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query2Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
                .filter(x -> x.f1.equals("comment"))
                .map(x -> Query2Parser.removeCommentType(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .sum(1);


        //Week statistics
        DataStream<Tuple2<String, Integer>> countWeekly=countHours
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
                .sum(1);

        //Monthly statistics
        DataStream<Tuple2<String, Integer>> countMonthly=countHours
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(30),Time.days(1)))
                .sum(1);

        //Getting result
        countHours
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new Query2Result("commentdaily.csv",0));

        countWeekly
                .timeWindowAll(Time.milliseconds(1))
                .apply(new Query2Result("commentweekly.csv",604800000-1));

        countMonthly
                .timeWindowAll(Time.milliseconds(1))
                .apply(new Query2Result("commentmonthly.csv",2592000000L-1));
    }
}
