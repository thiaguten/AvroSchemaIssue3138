package org.example;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class AvroSchemaIssueTest {

    @Test
    public void avroSchemaIssueTest() throws Exception {
        String expectedSchema = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"User\",\n" +
                "  \"namespace\" : \"org.example.AvroSchemaIssueTest\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"name\",\n" +
                "    \"type\" : \"string\"\n" +
                "  }, {\n" +
                "    \"name\" : \"favoriteNumber\",\n" +
                "    \"type\" : \"int\"\n" +
                "  }, {\n" +
                "    \"name\" : \"favoriteColor\",\n" +
                "    \"type\" : \"string\"\n" +
                "  } ]\n" +
                "}";

        Schema schema = createSchema(User.class, false);
        Assertions.assertThat(schema).isNotNull();
        String actualSchema = schema.toString(true);

        // Works with avro 1.9.2, but fails with 1.10.x and 1.11.0 because the order of
        // the record fields in the generated schema has been changed.
        Assertions.assertThat(actualSchema).isEqualToIgnoringNewLines(expectedSchema);

        String json = "{\"name\":\"Thiago\",\"favoriteNumber\":777,\"favoriteColor\":\"Blue\"}";
        User user = new User("Thiago", 777, "Blue"); // JsonMapper.builder().build().readValue(json, User.class);

        // Serialize Class to Avro:
        byte[] avroBytes = toAvro(user, schema);
        System.out.println("Serialize Class to Avro: " + Arrays.toString(avroBytes));

        // Serialize JSON to Avro:
        byte[] avroBytes1 = fromJsonToAvro(json, schema);
        System.out.println("Serialize JSON to Avro: " + Arrays.toString(avroBytes1));

        // Deserialize Avro to Class:
        User user1 = fromAvro(avroBytes, schema);
        System.out.println("Deserialize Avro to Class: " + user1);

        // Deserialize Avro to JSON:
        String json1 = fromAvroToJson(avroBytes1, schema);
        System.out.println("Deserialize Avro to JSON: " + json1);

        // @formatter:off
        // -------------------------------------------------------
        //  T E S T S - Avro 1.9.2
        // -------------------------------------------------------
        // Running org.example.AvroSchemaIssueTest
        // Serialize Class to Avro: [12, 84, 104, 105, 97, 103, 111, -110, 12, 8, 66, 108, 117, 101]
        // Serialize JSON to Avro: [12, 84, 104, 105, 97, 103, 111, -110, 12, 8, 66, 108, 117, 101]
        // Deserialize Avro to Class: User [favoriteColor=Blue, favoriteNumber=777, name=Thiago]
        // Deserialize Avro to JSON: {"name":"Thiago","favoriteNumber":777,"favoriteColor":"Blue"}
        // Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.365 sec
        //
        // -------------------------------------------------------
        //  T E S T S - Avro 1.10.2
        // -------------------------------------------------------
        // Running org.example.AvroSchemaIssueTest
        // Serialize Class to Avro: [8, 66, 108, 117, 101, -110, 12, 12, 84, 104, 105, 97, 103, 111]
        // Serialize JSON to Avro: [8, 66, 108, 117, 101, -110, 12, 12, 84, 104, 105, 97, 103, 111]
        // Deserialize Avro to Class: User [favoriteColor=Blue, favoriteNumber=777, name=Thiago]
        // Deserialize Avro to JSON: {"favoriteColor":"Blue","favoriteNumber":777,"name":"Thiago"}
        // Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.394 sec
        //
        // -------------------------------------------------------
        //  T E S T S - Avro 1.11.0
        // -------------------------------------------------------
        // Running org.example.AvroSchemaIssueTest
        // Serialize Class to Avro: [8, 66, 108, 117, 101, -110, 12, 12, 84, 104, 105, 97, 103, 111]
        // Serialize JSON to Avro: [8, 66, 108, 117, 101, -110, 12, 12, 84, 104, 105, 97, 103, 111]
        // Deserialize Avro to Class: User [favoriteColor=Blue, favoriteNumber=777, name=Thiago]
        // Deserialize Avro to JSON: {"favoriteColor":"Blue","favoriteNumber":777,"name":"Thiago"}
        // Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.387 sec
        // @formatter:on
    }

    public static Schema createSchema(Class<?> schemaType, boolean allowNull) {
        return allowNull
                ? ReflectData.AllowNull.get().getSchema(schemaType)
                : ReflectData.get().getSchema(schemaType);
    }

    public static <T> byte[] toAvro(T object, Schema schema) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(object, encoder);
            encoder.flush();
            outputStream.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromAvro(byte[] payload, Schema schema) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(payload)) {
            DatumReader<T> datumReader = new ReflectDatumReader<>(schema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            T datum = null;
            while (!binaryDecoder.isEnd()) {
                datum = datumReader.read(datum, binaryDecoder);
            }
            return datum;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String fromAvroToJson(byte[] avroBytes, Schema schema) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
            DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            Object datum = null;
            while (!binaryDecoder.isEnd()) {
                datum = datumReader.read(datum, binaryDecoder);
                datumWriter.write(datum, jsonEncoder);
                jsonEncoder.flush();
            }
            outputStream.flush();
            return outputStream.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] fromJsonToAvro(String json, Schema schema) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
            DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, json);
            Object datum = null;
            while (true) {
                try {
                    datum = datumReader.read(datum, jsonDecoder);
                } catch (EOFException eofException) {
                    break;
                }
                datumWriter.write(datum, binaryEncoder);
                binaryEncoder.flush();
            }
            outputStream.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class User {
        private String name;
        private Integer favoriteNumber;
        private String favoriteColor;

        public User() {
            super();
        }

        public User(String name, Integer favoriteNumber, String favoriteColor) {
            this.name = name;
            this.favoriteNumber = favoriteNumber;
            this.favoriteColor = favoriteColor;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getFavoriteNumber() {
            return favoriteNumber;
        }

        public void setFavoriteNumber(Integer favoriteNumber) {
            this.favoriteNumber = favoriteNumber;
        }

        public String getFavoriteColor() {
            return favoriteColor;
        }

        public void setFavoriteColor(String favoriteColor) {
            this.favoriteColor = favoriteColor;
        }

        @Override
        public String toString() {
            return "User [favoriteColor=" + favoriteColor + ", favoriteNumber=" + favoriteNumber + ", name=" + name
                    + "]";
        }
    }
}
