package flink.metrics;

import flink.utils.other.FileUtils;
import kafka.ProducerKafka;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class LatencyTracker {

    public static void computeLatency(long start, long end, int index,String kafkaAddress) throws IOException {
        Producer<String, String> producer = ProducerKafka.setConfig(kafkaAddress);
        double latency = (end - start)/Math.pow(10,6);
        System.out.println(latency);


        switch (index){
            case 1:
                ProducerKafka.produce(producer,latency+"\n","query1latency");
                producer.close();
                break;


            case 2:
                ProducerKafka.produce(producer,latency+"\n","query2latency");
                producer.close();
                break;

            case 3:
                ProducerKafka.produce(producer,latency+"\n","query3latencydir");
                producer.close();

                break;
            case 4:
                ProducerKafka.produce(producer,latency+"\n","query3latencyind");
                producer.close();
                break;

                default:
                    break;
        }
    }
}
