package com.nilay.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerAssignApp {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);

        ArrayList<TopicPartition> partitions =  new ArrayList<TopicPartition>();
        TopicPartition myTopicPartition = new TopicPartition("my-topic",0);
        TopicPartition myOtherTopicPartition = new TopicPartition("my-other-topic",2);
        partitions.add(myTopicPartition);
        partitions.add(myOtherTopicPartition);

        kafkaConsumer.assign(partitions);

        try{
            while(true){
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
                for(ConsumerRecord<String,String> record : consumerRecords){
                    System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value:%s",record.topic()
                            ,record.partition(),record.offset(),record.key(),record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        finally {
            kafkaConsumer.close();
        }
    }
}
