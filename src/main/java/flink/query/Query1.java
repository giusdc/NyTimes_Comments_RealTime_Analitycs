package flink.query;

import flink.utils.flink.query1.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
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

     static transient Time timeout = Time.hours(2);
     static int lag=3600000*2-1;
     static int x=0;


   // public static long param=1514851200000L;
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){

        //for metric computing only
        /*
        stream.process(new ProcessFunction <Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>,Object> (){
            private transient DropwizardMeterWrapper meter;
            private transient Meter meter2;


            @Override
            public void processElement(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> tuple15, Context context, Collector<Object> collector) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
                this.meter.markEvent();
                double value=this.meter.getDropwizardMeter().getMeanRate();
                //System.out.println();
            }
        });*/

        /*
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(new CustomWindow(timeout.toMilliseconds()))
                //.window(TumblingEventTimeWindows.of(Time.hours(1)))
               // .trigger(new CustomTrigger(timeout))
                /*.trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> stringIntegerTuple2, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        if (l >=param) {
                            param=Long.MAX_VALUE;
                            return TriggerResult.FIRE;
                        } else
                            return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

                    }
                })


                .aggregate(new Query1Aggregate(), new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(iterable.iterator().next());
                    }
                });




        rank1h.timeWindowAll(Time.milliseconds(1)).apply(new AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                x++;
                if(x%2==0){
                    timeout=Time.days(2);
                    lag=3600000-1;
                }else
                {
                    timeout=Time.days(1);
                    lag=3600000*2-1;
                }

                System.out.println("Start "+(timeWindow.getStart()-lag)+" End "+(timeWindow.getEnd()));


            }
        });*/


        //Hour statistics
        DataStream<Tuple2<String, Integer>> rank1h = stream
                .filter(x->x.f0!=-1)
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankhourly.csv"));

        //Daily statistics
        DataStream<Tuple2<String, Integer>> rankDaily = rank1h
                         .keyBy(0)
                         .window(SlidingEventTimeWindows.of(Time.days(1),Time.hours(1)))
                         .aggregate(new Query1Aggregate(), new Query1Rank("rankdaily.csv"));

       //Week statistics
        DataStream<Tuple2<String, Integer>> rankWeek = rank1h
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
                //.window(SlidingEventTimeWindows.of(Time.days(7),Time.days(1), Time.days(-3)))
                .aggregate(new Query1Aggregate(), new Query1Rank("rankweekly.csv"));

        //Getting rank
       rank1h
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("rankhourly.csv",3600000-1));
       rankDaily
               .timeWindowAll(Time.milliseconds(1))
               .apply(
                new Query1RankWindows("rankdaily.csv",86400000-1));
        rankWeek
                .timeWindowAll(Time.milliseconds(1))
                .apply(
                new Query1RankWindows("rankweekly.csv",604800000-1));
    }
}




