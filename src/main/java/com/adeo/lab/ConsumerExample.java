package com.lecoufle.lab;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerExample {

    private static final String BROKER_KAFKA = "localhost:9092";
    private static final String TOPIC = "lab-kafka-basic";
    
    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_KAFKA);
        
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "LABB-KAFKA");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        Duration timeDuration = Duration.ofSeconds(5);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        int nb = 120;
        System.out.println("start");
        while(nb > 0) {
            ConsumerRecords<String, String> records = consumer.poll(timeDuration);
            for (ConsumerRecord<String, String> consumerRecord : records) {
                System.out.println(" This is a new message : {key: " + consumerRecord.key() + " , value: " + consumerRecord.value() + "} , offset : " + consumerRecord.offset());
            }
            nb--;
        }
        System.out.println("end");
        consumer.close();
    }
    
}
