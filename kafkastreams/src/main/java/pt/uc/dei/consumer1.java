package pt.uc.dei;

import java.time.Duration;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;  

  
public class consumer1 {  
    public static void main(String[] args) throws Exception{  
        //Logger logger= LoggerFactory.getLogger(consumer1.class.getName());  
        String bootstrapServers="127.0.0.1:9093";  
        String grp_id="third_app";  
        String topic="b-1";  
        //Creating consumer properties  
        Properties props = new Properties(); 
        //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092"); //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //creating consumer  
        Consumer<String,String> consumer= new KafkaConsumer<String,String>(props);  
        
        //Subscribing  
        consumer.subscribe(Arrays.asList(topic));  
        //polling  

        try {
            while (true) {
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));  
            for(ConsumerRecord<String,String> record: records){  
                //logger.info("Key: "+ record.key() + ", Value:" +record.value());  
                //logger.info("Partition:" + record.partition()+",Offset:"+record.offset());  
                System.out.println(record);
                }  
            }  
              
        }
        finally {
            consumer.close();
        }
        
        
    }  
}  