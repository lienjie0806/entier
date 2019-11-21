package com.entier.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class JProducer extends Thread {
    public static void main(String[] args) {
        JProducer jproducer = new JProducer();
        jproducer.start();
    }

    @Override
    public void run() {
        producer();
    }

    private void producer() {
        Properties props = config();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            String json = "{\"id\":" + i + ",\"ip\":\"192.168.0." + i + "\",\"date\":" + new Date().toString() + "}";
            String k = "key" + i;
            producer.send(new ProducerRecord<String, String>("flink_topic", k, json));
        }
        producer.close();
    }

    private Properties config() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "134.96.179.37:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
