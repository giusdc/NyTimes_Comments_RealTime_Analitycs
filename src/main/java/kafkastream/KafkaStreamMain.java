package kafkastream;

import flink.utils.flink.query2.Query2Parser;
import flink.utils.kafka.KafkaProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class KafkaStreamMain {
    public static void main(String[] args) {

        final Properties props = KafkaProperties.createStreamProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> commentsDaily = builder
                .stream("comments", Consumed.with(Serdes.String(),
                        Serdes.String(), new MyEventTimeExtractor(), Topology.AutoOffsetReset.EARLIEST))
                .map((x, y) -> Query2ParserKafkaStream.getKeyValue((y)))
                .filter((x, y) -> x.split("_")[2].equals("comment"))
                .map((x, y) -> Query2ParserKafkaStream.removeComment(x, y))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(2)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .selectKey((x, y) -> x.key())
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .reduce((x, y) -> x + y)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .selectKey((x, y) -> x.key());

        KStream<String, Long> commentsWeekly = commentsDaily
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                //.windowedBy(new KafkaWindow())
                .windowedBy(TimeWindows.of(Duration.ofHours(24 * 7)).advanceBy(Duration.ofHours(24)))
                .reduce((x, y) -> x + y)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .selectKey((x, y) -> x.key());

        KStream<String, Long> commentsMonthly = commentsDaily
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(24 * 7 * 30)).advanceBy(Duration.ofHours(24)))
                .reduce((x, y) -> x + y)
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .selectKey((x, y) -> x.key()); 
                //.print(Printed.toSysOut());




        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



/*
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);*/

    }


    }

