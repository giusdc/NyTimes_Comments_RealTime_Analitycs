package kafkastream.kafkaoperators;

import kafkastream.utils.SerializerUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import scala.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;

import static flink.utils.flink.query2.Query2Parser.getKey;

public class Query2ParserKafkaStream {

    public static KeyValue<String,Long> getKeyValue(String x) {

        String tuple[]=x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if(checkLine(tuple)) {
            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(tuple[5]) * 1000),
                            ZoneOffset.UTC.normalized());
            String key=getKeyString(triggerTime.getHour());
            return new KeyValue<>(key+"_"+tuple[4],1L);
        }else
            return new KeyValue<>(null,null);

    }

    //Remove the comment value
    public static KeyValue<String,Long> removeComment(String x, Long y) {
        String key = x.split("_")[0] +"_"+ x.split("_")[1];
        return new KeyValue<>(key,y);
    }

    //Add the window's start
    public static KeyValue<String,String> getKey(Windowed<String> x, Long y) {
        String key =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(x.window().start()),
                        ZoneOffset.UTC.normalized()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
        return new KeyValue<>(key,x.key()+":"+y);
    }







    private static String getKeyString(int hour) {

        String key="count_h";
        if(hour%2==0){
            if(hour<=9)
                key+="0"+hour;
            else
                key+=hour;
        }else{
            if(hour<=9)
                key+="0"+(hour-1);
            else
                key+=hour-1;
        }
        return key;
    }


    public static KeyValue<String,String> getLongArray(String x, byte[] y) {
        Long[] value = (Long[]) SerializerUtils.deserialize(y);
        String result="[";
        for (Long aLong : value) {
            result += aLong + ",";
        }
        result+="]";
        return new KeyValue<>(x, result);
    }

    //Check the line
    private static boolean checkLine(String[] str) {

        try {
            if (Long.parseLong(str[0]) < 1514764800)
                return false;
            if (Integer.parseInt(str[2]) < 0)
                return false;
            if (Integer.parseInt(str[3]) < 0)
                return false;
            if (!(str[4].equals("comment") || str[4].equals("userReply")))
                return false;
            if (Long.parseLong(str[5]) < 1514764800)
                return false;
            if (!(Integer.parseInt(str[6]) >= 1 && Integer.parseInt(str[6]) <= 3))
                return false;
            if (!(str[7].equals("False") || str[7].equals("True")))
                return false;
            if (str[4].equals("comment")) {
                if (!(str[8].equals("0") && str[9].equals("")))
                    return false;

            }else
            {
                if(Integer.parseInt(str[8])<0)
                    return false;
            }
            if (Integer.parseInt(str[10]) < 0)
                return false;
            if(Integer.parseInt(str[13])<0)
                return false;
        } catch (NumberFormatException num) {
            return false;
        }
        return true;

    }
}
