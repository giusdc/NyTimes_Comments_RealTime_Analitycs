package flink.metrics;

import flink.utils.kafka.CommentParser;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;
import java.time.Instant;

public class TopicDeserializationMetrics implements DeserializationSchema<Tuple16<Long, String, Long, Long, String,
        Long, Integer, String, Long, String, Long,String,String,Long,String, Long>> {
    @Override
    public Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> deserialize(byte[] message) throws IOException {
        String line = new String(message);
        return CommentParser.parseMetrics(line);
    }

    @Override
    public boolean isEndOfStream(Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long> longStringLongLongStringLongIntegerStringLongStringLongStringStringLongStringLongTuple16) {
        return false;
    }

    @Override
    public TypeInformation<Tuple16<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String, Long>> getProducedType() {
        return new TupleTypeInfo<>(TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class));
    }
}
