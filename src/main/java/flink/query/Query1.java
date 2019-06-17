package flink.query;

import flink.utils.Query1Parser;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query1 {
    //In seconds
    public static final int WINDOWS_LENGTH=86400000;
    public static final int SLINDING_WINDOWS_LENGHT=3600000;
    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream){
        DataStream<Tuple2<String, Integer>> sum = stream
                .map(x -> Query1Parser.parse(x))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOWS_LENGTH),Time.milliseconds(SLINDING_WINDOWS_LENGHT))
                .sum(1);
        sum.print();
    }
}
