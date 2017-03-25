package com.learning;

import com.sun.tools.javac.jvm.Gen;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by pthatchinamoorthy on 3/24/17.
 */
public class ConsumerClient {

    public static String TOPIC_NAME= "newTopic";

    public static void main(String args []) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "dineshConsumerGroupId");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String,GenericRecord> consumerRecords = consumer.poll(3000);
            for(ConsumerRecord<String,GenericRecord> record : consumerRecords) {
                String key = record.key();
                GenericRecord genericRecord = record.value();

                Schema schema = genericRecord.getSchema();

                long offset = record.offset();
                int partition = record.partition();
                long timestamp = record.timestamp();

                System.out.println("Key: " + key);
                System.out.println("Value: " + genericRecord);
                System.out.println("year: " + genericRecord.get("year"));
                System.out.println("companyName: " + genericRecord.get("companyName"));

                System.out.println("offset: " + offset);
                System.out.println("partition: " + partition);
                System.out.println("timestamp: " + timestamp);
            }
        }

    }

}
