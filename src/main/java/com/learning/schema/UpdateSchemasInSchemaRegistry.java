package com.learning.schema;

import com.homeaway.commons.logging.events.AccessLogEvent;
import com.homeaway.commons.logging.events.EventHeader;
import com.homeaway.commons.logging.events.user.*;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroEncoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by pthatchinamoorthy on 3/27/17.
 */
public class UpdateSchemasInSchemaRegistry {

    static String keySchemaString = "{\"type\":\"record\"," +
            "\"name\":\"keyRecord\"," +
            "\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}";

    public static void main(String[] args) throws IOException, RestClientException {
        //pushSchema("test");
        pushMessageToUserEventsCoreUI();
    }

    private static void pushSchema(String env) throws IOException, RestClientException {
        Properties properties = new Properties();
        properties.put("schema.registry.url", String.format("http://kafka-schema-%s.wvrgroup.internal:8081",env));
        KafkaAvroEncoder encoder = new KafkaAvroEncoder(new VerifiableProperties(properties));

        encoder.register("userSessions_COREUI-value", UserSession.SCHEMA$);
        encoder.register("userEvents_COREUI-value", UserEvent.SCHEMA$);

        encoder.register("userSessionEventJoin-value", UserSessionEventJoin.SCHEMA$);
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        encoder.register("userEvents_COREUI-value", parser.parse("{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.homeaway.commons.logging.events.user\",\"doc\":\"*  User event logged by services, webapps or from the UI\",\"fields\":[{\"name\":\"header\",\"type\":{\"type\":\"record\",\"name\":\"EventHeader\",\"namespace\":\"com.homeaway.commons.logging.events\",\"doc\":\"Generic event header to be used by most logging events\",\"fields\":[{\"name\":\"time\",\"type\":\"long\",\"doc\":\"Milliseconds since the epoc for this event\"},{\"name\":\"threadId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"The threadId of this event\",\"default\":null},{\"name\":\"requestMarker\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"The requestMarker (optional) for this event\",\"default\":null},{\"name\":\"env\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"The environment for this event\",\"default\":null},{\"name\":\"server\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"The host for this event (using LinkedIn's decoder/encoder fieldnames)\",\"default\":null},{\"name\":\"service\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"The application for this event  (using LinkedIn's decoder/encoder fieldnames)\",\"default\":null}]},\"doc\":\"* EventHeader of the log event.\"},{\"name\":\"clientRequestId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* A unique Id representing a client request that can be used to trace the call graph\"},{\"name\":\"sessionId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* A unique Id representing a user session ('has' session cookie)\"},{\"name\":\"visitorId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* A unique Id representing a homeaway visitor/user across sessions (\\\"hav\\\" persistent cookie)\",\"default\":null},{\"name\":\"type\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* The type of this event\",\"default\":null},{\"name\":\"requestInfo\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"RequestInfo\",\"doc\":\"* Request info for UI events\",\"fields\":[{\"name\":\"remoteIp\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* IP Address of the client (remote host) which made the request to the server\"},{\"name\":\"remoteIpInfo\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"IpInfo\",\"doc\":\"* IP Address Location Information\",\"fields\":[{\"name\":\"latitude\",\"type\":[\"null\",\"double\"],\"doc\":\"* Latitude of the Remote Ip, if available\",\"default\":null},{\"name\":\"longitude\",\"type\":[\"null\",\"double\"],\"doc\":\"* Longitude of the Remote Ip, if available\",\"default\":null},{\"name\":\"continent\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* Continent of the Remote Ip, if available\",\"default\":null},{\"name\":\"country\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* Country of the Remote Ip, if available\",\"default\":null},{\"name\":\"region\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* Region of the Remote Ip, if available\",\"default\":null},{\"name\":\"city\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* City of the Remote Ip, if available\",\"default\":null},{\"name\":\"postalCode\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* Postal Code of the Remote Ip, if available\",\"default\":null}]}],\"doc\":\"* Remote IP Address Info\",\"default\":null},{\"name\":\"url\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* URL sent to the server for UI events\"}]}],\"doc\":\"* request info for UI events\",\"default\":null},{\"name\":\"accessLog\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"AccessLogEvent\",\"namespace\":\"com.homeaway.commons.logging.events\",\"doc\":\"* Represents an event from access.log\",\"fields\":[{\"name\":\"localIp\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* IP Address of the server (Local Host)\"},{\"name\":\"remoteIp\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* IP Address of the client (remote host) which made the request to the server\"},{\"name\":\"time\",\"type\":\"long\",\"doc\":\"* Date and Time of the request as ms since the epoch in GMT\"},{\"name\":\"method\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* Request method.\"},{\"name\":\"url\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* URL sent to the server\"},{\"name\":\"protocolAndVersion\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* Protocol and Version\"},{\"name\":\"statusCode\",\"type\":\"int\",\"doc\":\"* HTTP status code according to RFC 2616\"},{\"name\":\"numBytes\",\"type\":\"int\",\"doc\":\"* Number of bytes in the request\"},{\"name\":\"applicationRuntime\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"* The application runtime and version (e.g. Java/1.6.0_16)\"}]}],\"doc\":\"* access log event for service/webapp events\",\"default\":null},{\"name\":\"payload\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* Generic payload, schema on read\",\"default\":null},{\"name\":\"payloadMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}],\"doc\":\"* Mapped payload data\",\"default\":null},{\"name\":\"currentPageViewId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* A unique Id representing the current page that generated this event.\\n         * This should be used for the call graph\",\"default\":null},{\"name\":\"parentPageViewId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"* A unique Id representing the previous page. Used to follow travelers as they proceed through the funnel\",\"default\":null}]}"));
    }

    public  static void pushMessageToUserEventsCoreUI(){
        UserEvent userEvent = getUserEvent();

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema keySchema = parser.parse(keySchemaString);
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("key", "prabhakarKey");



        KafkaProducer<GenericRecord, UserEvent> producer = new KafkaProducer<>(getProducerProperties());

        try {
            RecordMetadata response = producer.send(new ProducerRecord<>("userEvents_QED", userEvent)).get();
            System.out.println("Message Sent" + ": offset-" + response.offset() + " : partition-" + response.partition());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static UserSession getUserSession() {
        AccessLogEvent accessLog = AccessLogEvent.newBuilder()
                .setApplicationRuntime("app")
                .setLocalIp("IP")
                .setMethod("GET")
                .setTime(Instant.now().toEpochMilli())
                .setRemoteIp("0.9.0.0")
                .setUrl("http://")
                .setLocalIp("0.0.0")
                .setNumBytes(0)
                .setProtocolAndVersion("")
                .setStatusCode(1)
                .build();

        EventHeader eventHeader = EventHeader.newBuilder()
                .setTime(Instant.now().toEpochMilli())
                .setEnv("test")
                .setRequestMarker("")
                .setServer("")
                .setService("")
                .setThreadId("")
                .build();

        IpInfo ipInfo = IpInfo.newBuilder().setCity("").setContinent("").setCountry("").setLatitude(0.0).setLongitude(0.0).setPostalCode("").setRegion("").build();
        RequestInfo requestInfo = RequestInfo.newBuilder()
                .setRemoteIpInfo(ipInfo)
                .setRemoteIp("")
                .setUrl("")
                .build();

        return UserSession.newBuilder()
                .setHeader(eventHeader)
                .build();
    }

    private static UserEvent getUserEvent() {
        AccessLogEvent accessLog = AccessLogEvent.newBuilder()
                .setApplicationRuntime("app")
                .setLocalIp("IP")
                .setMethod("GET")
                .setTime(Instant.now().toEpochMilli())
                .setRemoteIp("0.9.0.0")
                .setUrl("http://")
                .setLocalIp("0.0.0")
                .setNumBytes(0)
                .setProtocolAndVersion("")
                .setStatusCode(1)
                .build();

        EventHeader eventHeader = EventHeader.newBuilder()
                .setTime(Instant.now().toEpochMilli())
                .setEnv("test")
                .setRequestMarker("")
                .setServer("")
                .setService("")
                .setThreadId("")
                .build();

        IpInfo ipInfo = IpInfo.newBuilder().setCity("").setContinent("").setCountry("").setLatitude(0.0).setLongitude(0.0).setPostalCode("").setRegion("").build();
        RequestInfo requestInfo = RequestInfo.newBuilder()
                .setRemoteIpInfo(ipInfo)
                .setRemoteIp("")
                .setUrl("")
                .build();

        return UserEvent.newBuilder()
                .setAccessLog(accessLog)
                .setClientRequestId("")
                .setCurrentPageViewId("")
                .setHeader(eventHeader)
                .setParentPageViewId("")
                .setPayload("")
                .setPayloadMap(null)
                .setRequestInfo(requestInfo)
                .setSessionId("prasessionid")
                .setType("")
                .setVisitorId("visiptra")
                .build();
    }

    public static Properties getProducerProperties(){
        Properties config = new Properties();
        config.put("client.id", "prabhakarClient");
        config.put("bootstrap.servers", "astkaf100:9092");
        config.put("schema.registry.url", "http://astkaf100:8081");
        config.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        return config;
    }
}