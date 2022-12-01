package pt.uc.dei;


import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class producer1 {
 public static void main(String args[]){
    
    // Properties properties = new Properties();
    // properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Properties props = new Properties();
    props.put("bootstrap.servers", "broker1:9092");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String,String> first_producer = new KafkaProducer<String, String>(props); 

    ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_first", "");

    first_producer.send(record);
    first_producer.flush();
    first_producer.close();
}  
}