package kafkastream;

import flink.utils.kafka.KafkaProperties;
import kafkastream.kafkaoperators.MyEventTimeExtractor;
import kafkastream.kafkaoperators.Query2Aggregator;
import kafkastream.kafkaoperators.Query2Inizializer;
import kafkastream.kafkaoperators.Query2ParserKafkaStream;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class KafkaStreamMain {
    public static void main(String[] args) {

        final Properties props = KafkaProperties.createStreamProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Windowed<String>, Long> windowcommentsDaily = builder
                .stream("comments", Consumed.with(Serdes.String(),
                        Serdes.String(), new MyEventTimeExtractor(), Topology.AutoOffsetReset.EARLIEST))
                .map((x, y) -> Query2ParserKafkaStream.getKeyValue((y)))
                .filter((x,y)->x!=null)
                .filter((x, y) -> x.split("_")[2].equals("comment"))
                .map(Query2ParserKafkaStream::removeComment)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(2)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .selectKey((x, y) -> x.key())
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .reduce(Long::sum)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream();

        KStream<String, Long> commentsDaily = windowcommentsDaily
                .selectKey((x, y) -> x.key());


        //Produce daily result

        windowcommentsDaily
                .map(Query2ParserKafkaStream::getKey)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(new Query2Inizializer(), new Query2Aggregator())
                .toStream()
                .map(Query2ParserKafkaStream::getLongArray)
                .print(Printed.toFile("daily"));
        //Week
       commentsDaily
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(24 * 7)).advanceBy(Duration.ofHours(24)))
                .reduce(Long::sum)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map(Query2ParserKafkaStream::getKey)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(new Query2Inizializer(), new Query2Aggregator())
                .toStream()
                .map(Query2ParserKafkaStream::getLongArray)
                .print(Printed.toFile("week"));

        //Monthly
        commentsDaily
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                //.windowedBy(TimeWindows.of(Duration.ofHours(24 * 30)).advanceBy(Duration.ofHours(24)))
                .windowedBy(TimeWindows.of(Duration.ofHours(24*30)).advanceBy(Duration.ofHours(24)))
                .reduce((x, y) -> x + y)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map(Query2ParserKafkaStream::getKey)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(new Query2Inizializer(), new Query2Aggregator())
                .toStream()
                .map(Query2ParserKafkaStream::getLongArray)
                .print(Printed.toFile("month"));





        //Start Kafka Streams
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


    }

