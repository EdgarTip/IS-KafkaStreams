package pt.uc.dei.streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.FloatSerde;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaStream {
    public static void main(String[] args) throws InterruptedException, IOException {
        
        final String inputTopic = "standard2";
        final String inputTopic2 = "alert2";
        final String outputTopic = "result-topic1";
        
        Properties streamProps = new Properties();

        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "id1");
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
        mainStreamStandard.map((k, v) -> new KeyValue<>(v.split(":")[0], v.split(":")[1]))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .count()
                        .toStream()
                        .filter((key, value) -> value != null)
                        .peek((key, value) -> System.out.println("-2- Outgoind record - key " + key + " value " + value))
                        .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


        // Get	minimum	and	maximum	temperature	per	weather	station. (EDGAR)



        // Get	minimum	and	maximum	temperature	per	location	(Students	should	computethese	values	in	Fahrenheit). (ALEXY)

        mainStreamStandard.map((k, v) -> new KeyValue<>(v.split(":")[0], v.split(":")[1]))
                        .groupByKey()
                        .aggregate( () -> new int[]{-200, -200}, (aggKey, newVal, aggValue) -> {
                        if(aggValue[0] == -200){
                            aggValue[0] = Integer.valueOf(newVal);
                            aggValue[1] = Integer.valueOf(newVal);
                        }
                        else{
                            aggValue[0] = Math.min(aggValue[0], Integer.valueOf(newVal));
                            aggValue[1] = Math.max(aggValue[1], Integer.valueOf(newVal));
                        }
                            
                        return aggValue;
                        }, Materialized.with(Serdes.String(), new IntArraySerde()))
                    .mapValues(v -> "Min " + (v[0] * 1.9 + 32) + " F / Max " + (v[1] * 1.9 + 32) + " F")
                    .toStream()
                    .filter((key, value) -> value != null)
                    .peek((key, value) -> System.out.println("-4-Outgoing record - key " + key + " value " + value))
                    .to(outputTopic + "-4", Produced.with(Serdes.String(), Serdes.String()));
 
        // Count	the	total	number	of	alerts	per	weather	station (EDGAR)



        //  Count	the	total	alerts	per	type. (ALEXY)

        mainStreamAlert.map((k, v) -> new KeyValue<>(k, v.split(":")[1]))
                        .groupByKey()
                        .aggregate( () -> new int[]{0, 0}, (aggKey, newVal, aggValue) -> {
                            if(newVal.equals("clear")){
                                aggValue[0] += 1;
                            }
                            else{
                                System.out.println("-->" + newVal + "<--");
                                aggValue[1] += 1;
                            }
                            return aggValue;
                        }, Materialized.with(Serdes.String(), new IntArraySerde()))             

                    .mapValues(v -> "Clear: " + v[0] + " / Extreme " + v[1])
                    .toStream()
                    .filter((key, value) -> value != null)
                    .peek((key, value) -> System.out.println("-6-Outgoing record - key " + key + " value " + value))
                    .to(outputTopic + "-6", Produced.with(Serdes.String(), Serdes.String()));

        // Get	minimum	temperature of	weather	stations	with	red	alert	events. (EDGAR)



        //Get	 maximum	 temperature	 of	 each	 location	 of	 alert	 events	 for the	 last	 hour	(students	are	allowed	to	define a	different	value	for	the	time	window). (EDGAR)



        // Get	minimum	temperature	per	weather	station	in	red	alert	zones. (ALEXY)



        // Get	the	average	temperature	per	weather	station. (EDGAR)



        // Get	the	average	temperature	of	weather	stations	with	red	alert	events	for the	last	hour	(students	are	allowed	to	define	a	different	value	for	the	time	window). (ALEXY)

        // Time window of 1 hour
        Duration windowSize = Duration.ofMinutes(1);
        Duration advanceSize = Duration.ofMinutes(1);
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);

        // Aggregate to get [sum, count] in the last time window
        KTable<String, int[]> averageTemp = mainStreamStandard.groupByKey()
        .aggregate( () -> new int[]{0 ,0}, (aggKey, newVal, aggValue) -> {
        aggValue[0] += Integer.valueOf(newVal.split(":")[1]);
        aggValue[1] += 1;  
        return aggValue;
        }, Materialized.with(Serdes.String(), new IntArraySerde()));
        
        // Join weather stations with their [sum,count] and their respective red alert events
        KStream<String, String> joined = mainStreamAlert.join(averageTemp,
        (leftValue, rightValue) -> "left/" + leftValue + "/right/" + (float)rightValue[0]/rightValue[1]); // left value = key, right value = [sum, count] -> [0]/[1] = average);

        // Key = Red alert event, Value = Average temperature
        KStream<String, String> avgStream = joined.map((k, v) -> new KeyValue<>(k, v.split("/")[3])); // value = average
         
        avgStream
        .groupByKey()
        .windowedBy(hoppingWindow)
        .aggregate( () -> "", (aggKey, newVal, aggValue) -> {
            return newVal;
        }, Materialized.with(Serdes.String(), new StringSerde()))   
        .toStream()
        .mapValues(v -> "Average: " + v)
        .map((wk, value) -> KeyValue.pair(wk.key(),value))
        .filter((key, value) -> value != null)
        .peek((key, value) -> System.out.println("-11-Outgoing record - key " + key + " value " + value))
        .to(outputTopic + "-11", Produced.with(Serdes.String(), Serdes.String()));

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
