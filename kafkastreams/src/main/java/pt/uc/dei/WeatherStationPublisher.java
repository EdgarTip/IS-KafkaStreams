package pt.uc.dei;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WeatherStationPublisher {
    public static void main(String[] args) throws Exception{ //Assign topicName to string variable

        String topicName = "standard2";
        String topicName2 = "alert2";

        String[] weatherStations = {"WS1", "WS2", "WS3", "WS4", "WS5", "WS6"};
        String[] locations={"Coimbra", "Miranda", "Lousa", "Guarda", "Viseu"};
        String[] red_type ={ "thunderstorm", "hurricane", "blizzard", "drought", "clear"};
      	Random r=new Random();        
      	
        int upperbound = 35;
        int lowerbound = 0;

        // create instance for properties to access producer configs
        Properties props = new Properties(); //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092");
        //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send to standard-weather topic some temperature readings
        for(int i = 0; i < 20; i++) {
            int id_weatherStation = r.nextInt(weatherStations.length);
            int id_location = r.nextInt(locations.length);
            Long temperature = (long)r.nextInt(upperbound-lowerbound) + lowerbound;
            producer.send(new ProducerRecord<String, String>(topicName, weatherStations[id_weatherStation], locations[id_location] + ":" + String.valueOf(temperature)));
            
            System.out.println("Sending message " + locations[id_location] + ":" + String.valueOf(temperature) + " to topic " + topicName);
        }

        producer.close();

        Producer<String, String> producer2 = new KafkaProducer<>(props);
        //Send to topic alert-weather some alerts
        for(int i = 0; i < 10; i++) {
            int id_weatherStation = r.nextInt(weatherStations.length);
            int id_location = r.nextInt(locations.length);
            int red_type_id = r.nextInt(red_type.length);
            producer2.send(new ProducerRecord<String, String>(topicName2,  weatherStations[id_weatherStation], locations[id_location] + ":" + red_type[red_type_id]));
            
            System.out.println("Sending message " + locations[id_location] + ":" + red_type[red_type_id] + " to topic " + topicName2);
        }
        producer2.close();
        

        
    }
}