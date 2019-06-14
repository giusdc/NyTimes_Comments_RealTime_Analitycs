package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class ProducerKafka {


    public static void produce(Producer<String, String> producer, String line) throws IOException {
        producer.send(new ProducerRecord<String, String>("comments", line));
    }


    public static Producer<String, String> setConfig() {
        //Set properties
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for ProducerKafka requests.
        props.put("acks", "all");

        //If the request fails, the ProducerKafka can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the ProducerKafka for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.create.topics.enable", true);

        return new KafkaProducer<>(props);
    }
}



