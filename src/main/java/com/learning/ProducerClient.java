package com.learning;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class ProducerClient
{
    public static String TOPIC_NAME= "newTopic";

    public static void main(String [] args) {



        Properties config = new Properties();
        config.put("client.id", "dineshClient");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        String schemaDescription = " {    \n"
                + " \"name\": \"company\", \n"
                + " \"type\": \"record\",\n" + " \"fields\": [\n"
                + "   {\"name\": \"companyName\", \"type\": \"string\"},\n"
                + "   {\"name\": \"year\", \"type\": \"int\"} ]\n" + "}";

        Schema schema = Schema.parse(schemaDescription);

        GenericRecord company = new GenericData.Record(schema);
        company.put("companyName", new Utf8("cognizant"));
        company.put("year", 1996);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(config);

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<String, GenericRecord>(TOPIC_NAME, "firstCompany", company);
        int i=0;
        while (i < 500) {
            producer.send(message);
            i++;
        }

    }
}
