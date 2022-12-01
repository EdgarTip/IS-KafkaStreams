package pt.uc.dei.streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class BasicStreams {
    public static void main(String[] args) throws InterruptedException, IOException {
       Properties streamProps = new Properties();

        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-d");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        final String inputTopic = "in";
        final String outputTopic = "out";

        final String orderNumberStart = "orderNumber-";
        
        KStream<String, Long> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Long()));
        firstStream.peek((key, value) -> System.out.println("key " + key + " value " + value))
                   .mapValues(value -> "" + value)
                   .filter((key, value) -> Long.parseLong(value) > 200)
                   .peek((key, value) -> System.out.println("key " + key + " value " + value))
                   .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProps);
        kafkaStreams.start();
        
    }   
}
