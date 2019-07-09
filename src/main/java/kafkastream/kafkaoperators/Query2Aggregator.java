package kafkastream.kafkaoperators;

import kafkastream.utils.SerializerUtils;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Arrays;

public class Query2Aggregator implements Aggregator<String, String, byte[]> {
    //Update the results
    @Override
    public byte[] apply(String s, String s2, byte[] agg) {
        Long[] records = (Long[]) SerializerUtils.deserialize(agg);
        records[getIndex(s2.split(":")[0])] = Long.parseLong(s2.split(":")[1]);
        return SerializerUtils.serialize(records);
    }
    private int getIndex(String s) {
        String[] key = {"count_h00", "count_h02", "count_h04", "count_h06", "count_h08", "count_h10", "count_h12", "count_h14", "count_h16", "count_h18", "count_h20", "count_h22"};
        //Get the index for setting the result
        return Arrays.asList(key).indexOf(s);
    }
}
