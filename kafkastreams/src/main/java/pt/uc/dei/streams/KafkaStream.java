package pt.uc.dei.streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaStream {
    public static void main(String[] args) throws InterruptedException, IOException {
        
        final String inputTopic = "are";
        final String inputTopic2 = "alertabcdesafsafsadfsaf";
        final String outputTopic = "result-topic";
        
        Properties streamProps = new Properties();

        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-a");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> mainStreamStandard = builder.stream(inputTopic);
        KStream<String, String> mainStreamAlert = builder.stream(inputTopic2);

        /* Ver exemplos no Theoretical !!*/

        // Count temperature readings of standard weather events per weather station.
        mainStreamStandard.groupByKey()
                          .count()
                          .toStream()
                          .filter((key, value) -> value != null)
                          .peek((key, value) -> System.out.println("-1- Outgoind record - key " + key + " value " + value))
                          .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
                          
        // Count	temperature	readings	of	standard	weather	events	per	location 
        mainStreamStandard.map((k, v) -> new KeyValue<>(v.split("-")[0], v.split("-")[1]))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .count()
                        .toStream()
                        .filter((key, value) -> value != null)
                        .peek((key, value) -> System.out.println("-2- Outgoind record - key " + key + " value " + value))
                        .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


        // Get	minimum	and	maximum	temperature	per	weather	station. (EDGAR)



        // Get	minimum	and	maximum	temperature	per	location	(Students	should	computethese	values	in	Fahrenheit). (ALEXY)



        // Count	the	total	number	of	alerts	per	weather	station (EDGAR)



        //  Count	the	total	alerts	per	type. (ALEXY)



        // Get	minimum	temperature of	weather	stations	with	red	alert	events. (EDGAR)



        //Get	 maximum	 temperature	 of	 each	 location	 of	 alert	 events	 for the	 last	 hour	(students	are	allowed	to	define a	different	value	for	the	time	window). (EDGAR)



        // Get	minimum	temperature	per	weather	station	in	red	alert	zones. (ALEXY)



        // Get	the	average	temperature	per	weather	station. (EDGAR)



        // Get	the	average	temperature	of	weather	stations	with	red	alert	events	for the	last	hour	(students	are	allowed	to	define	a	different	value	for	the	time	window). (EDGAR)





        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProps);
        kafkaStreams.start();

        //APAGAR MAIS TARDE
        //Restos de experiencias que fui fazendo. Manter aqui pq têm cenas que podem dar jeito

        // firstStream.peek((key, value) -> System.out.println("key " + key + " value " + value))
        //            .mapValues(value -> "" + value)
        //            .filter((key, value) -> Long.parseLong(value) > 200)
        //            .peek((key, value) -> System.out.println("key " + key + " value " + value))
        //            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProps);
        // kafkaStreams.start();



        // KTable<String, String> firstKTable = builder.table(inputTopic, 
        //                                                 Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
        //                                                 .withKeySerde(Serdes.String())
        //                                                 .withValueSerde(Serdes.String()));

        // firstKTable.filter((key, value) -> value.contains(orderNumberStart))
        //            .mapValues(value -> value.substring(value.indexOf("-") + 1))
        //            .filter((key, value) -> Long.parseLong(value) > 500)
        //            .toStream()
        //            .peek((key, value) -> System.out.println("Outgoind record - key " + key + " value " + value))
        //            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProps);
        // kafkaStreams.start();
        /*
        */


        
    }   
}