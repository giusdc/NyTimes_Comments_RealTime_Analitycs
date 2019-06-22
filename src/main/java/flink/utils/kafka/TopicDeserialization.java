package flink.utils.kafka;



import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;



public class TopicDeserialization implements DeserializationSchema<Tuple15<Long, String, Long, Long, String,
        Long, Integer, String, Long, String, Long,String,String,Long,String>> {




    @Override
    public Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> deserialize(byte[] message) throws IOException {
        String line = new String(message);
        return CommentParser.parse(line);
    }

    @Override
    public boolean isEndOfStream(Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String> longStringLongLongStringLongIntegerStringLongStringLongStringStringLongStringTuple15) {
        return false;
    }

    @Override
    public TypeInformation<Tuple15<Long, String, Long, Long, String, Long, Integer, String, Long, String, Long, String, String, Long, String>> getProducedType() {
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
                TypeInformation.of(String.class));
    }
}
