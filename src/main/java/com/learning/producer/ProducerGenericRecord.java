package com.learning.producer;

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
public class ProducerGenericRecord
{
    public static String TOPIC_NAME= "genericRecordExample";

    public static void main(String [] args) {


        KafkaProducer<String, GenericRecord> producer = getProducer();

        String schemaDescription = " {    \n"
                + " \"name\": \"Company\", \n"
                + " \"type\": \"record\",\n" + " \"fields\": [\n"
                + "   {\"name\": \"companyName\", \"type\": \"string\"},\n"
                + "   {\"name\": \"year\", \"type\": \"int\"} ]\n" + "}";

        Schema schema = Schema.parse(schemaDescription);

        GenericRecord company = new GenericData.Record(schema);
        company.put("companyName", new Utf8("cognizant"));
        company.put("year", 1996);



        ProducerRecord<String, GenericRecord> message = new ProducerRecord<String, GenericRecord>(TOPIC_NAME, "firstCompany", company);
        int i=0;
        while (i < 500) {
            producer.send(message);
            i++;
        }

    }

    public static KafkaProducer<String, GenericRecord> getProducer(){
        Properties config = new Properties();
        config.put("client.id", "prabhakarClient");
        config.put("bootstrap.servers", "astkaf100:9092");
        config.put("schema.registry.url", "http://astkaf100:8081");
        config.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(config);
        return producer;
    }
}
