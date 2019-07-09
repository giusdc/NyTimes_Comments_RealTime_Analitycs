package kafkastream.kafkaoperators;

import kafkastream.utils.SerializerUtils;
import org.apache.kafka.streams.kstream.Initializer;

public class Query2Inizializer implements Initializer<byte[]> {
    @Override
    public byte[] apply() {
        Long[] list = new Long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};
        return SerializerUtils.serialize(list);
    }
}
