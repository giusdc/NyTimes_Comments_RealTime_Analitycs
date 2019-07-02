package kafkastream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
        long timestamp = -1;
        timestamp =  Long.parseLong(consumerRecord.value().toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[5])*1000;

        if (timestamp < 0) {
            // Invalid timestamp!  Attempt to estimate a new timestamp,
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }
        return timestamp;



    }
}
