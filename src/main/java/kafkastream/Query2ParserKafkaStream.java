package kafkastream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static flink.utils.flink.query2.Query2Parser.getKey;

public class Query2ParserKafkaStream {

    //TODO fare il controllo tuple errateeeeeee
    public static KeyValue<String,Long> getKeyValue(String x) {

        String tuple[]=x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(tuple[5])*1000),
                        ZoneOffset.UTC.normalized());
        String key=getKey(triggerTime.getHour());
        return new KeyValue<>(key+"_"+tuple[4],1L);
    }

    public static KeyValue<String,Long> removeComment(String x, Long y) {
        String key = x.split("_")[0] +"_"+ x.split("_")[1];
        return new KeyValue<>(key,y);
    }
}
