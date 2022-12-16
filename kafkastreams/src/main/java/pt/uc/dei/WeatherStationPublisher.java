package pt.uc.dei;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WeatherStationPublisher {

    //Upper and lower bound of temperatures
    private static int upperbound = 40;
    private static int lowerbound = -40;
    private static String[] red_type ={ "thunderstorm", "hurricane", "blizzard", "drought", "clear"};  

    private static List<String> locations = new ArrayList<String>(); 


    public void consumer()  
    {    

         //Assign topicName to string variable
         final String topicName = "DB-Info-Topic";
         // create instance for properties to access producer configs
         Properties props = new Properties();
         //Assign localhost id
         props.put("bootstrap.servers", "broker2:9093"); //Set acknowledgements for producer requests. props.put("acks", "all");
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
         Consumer<String, String> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName));
         
        try {
            while (true) {
                Duration d = Duration.ofSeconds(1);
                ConsumerRecords<String, String> records = consumer.poll(d);
                Boolean first = true;
                for (ConsumerRecord<String, String> record : records) {
                    if(first){
                        locations = new ArrayList<String>();
                        first = false;
                    } 
                    locations.add(record.value()); 
                }
            }    
        }
        finally {
            consumer.close();
        }
  
    }   


    public static void sendNormalData(int weatherStation, int quantity, Producer<String,String> producer, String topicName){
        // Send to standard-weather topic some temperature readings
        Random r = new Random();
        for(int i = 0; i < quantity; i++) {
            int id_location = r.nextInt(locations.size());
            Long temperature = (long)r.nextInt(upperbound-lowerbound) + lowerbound;
            producer.send(new ProducerRecord<String, String>(topicName, "WS" + String.valueOf(weatherStation), locations.get(id_location) + ":" + String.valueOf(temperature)));
            
            System.out.println("Sending message: " + "WS" + String.valueOf(weatherStation) + " - " + locations.get(id_location) + ":" + String.valueOf(temperature) + " to topic " + topicName);
        }
    }


    public static void sendAlertData(int weatherStation, int quantity, Producer<String,String> producer, String topicName){
        //Send to topic alert-weather some alerts
        Random r = new Random();   
        for(int i = 0; i < quantity; i++) {
            int id_location = r.nextInt(locations.size());
            int red_type_id = r.nextInt(red_type.length);
            producer.send(new ProducerRecord<String, String>(topicName,  "WS" + String.valueOf(weatherStation), locations.get(id_location) + ":" + red_type[red_type_id]));
            
            System.out.println("Sending message " + "WS" + String.valueOf(weatherStation) + " - " + locations.get(id_location) + ":" + red_type[red_type_id] + " to topic " + topicName);
        }
    }

    public static void main(String[] args) throws Exception{ //Assign topicName to string variable

        String topicName = "t1-d";
        String topicName2 = "t2-d";           

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
        Producer<String, String> producer2 = new KafkaProducer<>(props);

        Scanner sc = new Scanner(System.in);
        while(true){
            System.out.println("""
                1-x-y to add normal data to a weather station (where x is the weather station to send the data and y the amount of data)\n
                2-x-y to add alert data to a  weather station (where x is the weather station to send the alerts and y the amount of data)\n
                0- exit aplication
               """);
            
            String option = sc.nextLine();

            
            if(option.split("-")[0].equals("0"))break;
            
            String[] locationsAux={"Coimbra", "Miranda", "Lousa", "Guarda", "Viseu"};

            for(int i =0; i < locationsAux.length; i++){
                locations.add(locationsAux[i]);
            }
            try{
                switch(option.split("-")[0]){
                    case "1":
                        sendNormalData(Integer.valueOf(option.split("-")[1]), Integer.valueOf(option.split("-")[2]), producer, topicName);
                        break;
                    case "2":
                        sendAlertData(Integer.valueOf(option.split("-")[1]), Integer.valueOf(option.split("-")[2]), producer2, topicName2);
                        break;
                    
                    default:
                        System.out.println("Not a valid input");
                        break;
                }
            }
            catch(Exception e){
                System.out.println("Not a valid input HERE");
                System.out.println(e);
            }
        }
        
        sc.close();
        producer.close();
        producer2.close();
    }
}