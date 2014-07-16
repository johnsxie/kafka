package tms.kafka.examples;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.BasicConfigurator; 

public class TestProducer {
    public static void main(String[] args) {
        try {  
        	BasicConfigurator.configure();
        	
        	long events = 5;
        	Random rnd = new Random();
 
        	Properties props = new Properties();
        	props.put("metadata.broker.list", "localhost:9092,localhost:9093");
        	props.put("serializer.class", "kafka.serializer.StringEncoder");
        	props.put("partitioner.class", "tms.kafka.examples.SimplePartitioner");
        	props.put("request.required.acks", "1");
           	ProducerConfig config = new ProducerConfig(props);
 
        	Producer<String, String> producer = new Producer<String, String>(config);
 
        	for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = new String("192.168.2.") + rnd.nextInt(255); 
               String msg = runtime + new String(",www.example.com,") + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
               producer.send(data);
        	}
            producer.close();
        	
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
}


