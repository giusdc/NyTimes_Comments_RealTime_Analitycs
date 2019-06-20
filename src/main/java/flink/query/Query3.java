package flink.query;

import flink.utils.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import static flink.MainFlink.WINDOWS_LENGTH;

public class Query3 {

    public static void process(DataStream<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> stream) {

        DataStream<Tuple2<Long, Float>> rank1h = stream
                .map(x -> Query2Parser.parse(x))
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.LONG))
                .keyBy(0)
                .timeWindow(Time.milliseconds(WINDOWS_LENGTH))
                .aggregate(new Query3Process(), new Query3Rank());
        rank1h.timeWindowAll(Time.milliseconds(1)).apply(
                new Query3RankWindows("pophourly.csv"));

    }
}
