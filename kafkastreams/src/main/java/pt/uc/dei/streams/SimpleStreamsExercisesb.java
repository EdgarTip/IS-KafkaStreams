package pt.uc.dei.streams;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

public class SimpleStreamsExercisesb {
    public static void main(String[] args) throws InterruptedException, IOException {         

        final String topicName = "standard-weather";
        final String outtopicname = "result-topic";

        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exercises-application-b");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        
        StreamsBuilder builder = new StreamsBuilder(); KStream<String, Long> lines = builder.stream(topicName);
        KTable<String, Long> outlines = lines.groupByKey().count();
        outlines.mapValues(v -> "" + v).toStream().to(outtopicname, Produced.with(Serdes.String(), Serdes.String()));

        /* hopping window */
        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceSize = Duration.ofSeconds(1);
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);
        lines
            .groupByKey()
            .windowedBy(hoppingWindow)
            .count()
            .toStream((wk, v) -> wk.key())
            .mapValues((k, v) -> k + " -> " + v)
            .to(outtopicname + "-6", Produced.with(Serdes.String(), Serdes.String()));

            
        KafkaStreams streams = new KafkaStreams(builder.build(), props); streams.start();
    } 
}