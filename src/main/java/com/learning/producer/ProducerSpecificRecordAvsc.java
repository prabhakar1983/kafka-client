package com.learning.producer;

import com.learning.domain.avsc.Company;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class ProducerSpecificRecordAvsc
{
    public static String TOPIC_NAME= "specificRecordExampleAvsc";

    public static void main(String [] args) {



        Properties config = new Properties();
        config.put("client.id", "dineshClient");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");


        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(config);

        Company companyAvdl = Company.newBuilder().setCompanyName("hopefullybetterwork").setYear(2026).build();

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<String, GenericRecord>(TOPIC_NAME, "thirdCompany", companyAvdl);
        int i=0;
        while (i < 500) {
            producer.send(message);
            i++;
        }

    }
}
