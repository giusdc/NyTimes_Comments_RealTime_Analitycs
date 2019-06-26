package flink.utils.flink.query3;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple5;

public class Query3Parser {
    public static Tuple5<Long, String,String,Long,Long> parse(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> x) {
        return new Tuple5<Long,String, String, Long,Long>(x.f13,x.f4,x.f7,x.f10,x.f8);
    }

    public static Tuple5<Long, String,String,Long,Long> changeKey(Tuple5<Long, String, String, Long, Long> x) {

        if(x.f1.equals("comment"))
            return new Tuple5<Long,String, String, Long,Long>(x.f0,x.f1,x.f2,x.f3,x.f4);
        else
            return new Tuple5<Long,String, String, Long,Long>(x.f4,x.f1,x.f2,x.f3,x.f0);

    }
}
