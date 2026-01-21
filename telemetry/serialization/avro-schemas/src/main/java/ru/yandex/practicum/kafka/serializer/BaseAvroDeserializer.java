package ru.yandex.practicum.kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    // ....

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        // ...
    }

    // ...

    @Override
    public T deserialize(String topic, byte[] data) {
        // Код десериализации двоичных данных
    }
}
