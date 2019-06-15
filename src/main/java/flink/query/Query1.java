package flink.query;

import flink.utils.Query1Parser;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query1 {
    public static void process(DataStreamSource<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){
        System.out.println("ECCOMIIIIIIIII");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple = stream.map(x -> Query1Parser.parse(x)).returns(Types.TUPLE(Types.STRING, Types.INT));
        tuple.writeAsText("prova");
        KeyedStream<Tuple2<String, Integer>, Tuple> sum = tuple
                .keyBy(0);
                //.timeWindow(Time.milliseconds(10))
                //.sum(1);
        sum.writeAsText("fine");
        System.out.println("finito");
    }
}
