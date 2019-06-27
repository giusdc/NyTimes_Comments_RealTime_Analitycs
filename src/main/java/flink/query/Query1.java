package flink.query;

import flink.utils.flink.query1.Query1Parser;
import flink.utils.flink.query1.Query1Aggregate;
import flink.utils.flink.query1.Query1Rank;
import flink.utils.flink.query1.Query1RankWindows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class Query1 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){



        //new MeterQuery1();
        //for metric computing only
        stream.process(new ProcessFunction <Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>,Object> (){
            private transient DropwizardMeterWrapper meter;
            private transient Meter meter2;


            @Override
            public void processElement(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15, Context context, Collector<Object> collector) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
                this.meter.markEvent();
                double value=this.meter.getDropwizardMeter().getMeanRate();
                System.out.println();
            }
        });





        //Hour statistic
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv"));

        //Daily statistic
     DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .window(TumblingEventTimeWindows.of(Time.days(1)))
                         .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv"));

       //Week statistic
        DataStream<Tuple2<String, Integer>> rankWeek = rankDaily
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
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
