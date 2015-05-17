package org.wkh.learningavro;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;

public class UserSerDe {
    private final DatumReader<GenericRecord> datumReader;
    private final User.Builder builder = User.newBuilder();
    private final DatumWriter<GenericRecord> datumWriter;

    public UserSerDe() {
        Schema classSchema = User.getClassSchema();
        datumReader = new GenericDatumReader<GenericRecord>(classSchema);
        datumWriter = new GenericDatumWriter<GenericRecord>(classSchema);
    }

    public User deserialize(byte[] bytes) throws Exception {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = datumReader.read(null, decoder);

        String name = ((Utf8) record.get("name")).toString();
        String favoriteColor = ((Utf8) record.get("favorite_color")).toString();
        Integer favoriteNumber = (Integer) record.get("favorite_number");

        return builder
                .setFavoriteColor(favoriteColor)
                .setFavoriteNumber(favoriteNumber)
                .setName(name)
                .build();
    }

    public byte[] serialize(User user) throws Exception {
        BinaryEncoder encoder = null;
        GenericData.Record record = new GenericData.Record(User.getClassSchema());

        record.put("favorite_color", user.getFavoriteColor());
        record.put("favorite_number", user.getFavoriteNumber());
        record.put("name", user.getName());

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(stream, encoder);

        datumWriter.write(record, encoder);
        encoder.flush();

        return stream.toByteArray();
    }
}
