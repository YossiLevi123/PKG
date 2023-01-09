package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args) throws FileNotFoundException {

        // setting properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> produce = new KafkaProducer<>(props);


        //reading file
        File read = new File("C:\\dev\\PKG-main\\src\\test\\java\\org\\example\\data.txt");
        Scanner scan = new Scanner(read);
        String data = null;
        while(scan.hasNextLine()){
            data = scan.nextLine();
        }

        scan.close();


        long uid = 6289875123065L;
        String metaData = "{\"a1\":\"A1\",\"a2\":\"1\",\"uid\":\"UID\"}";

        for (int i = 0; i < 100; i++) {

            String a1 = "a" + (i%3);
            String md = metaData.replace("UID", String.valueOf(uid++)).replace("A1", a1);

            String msg = md + "\n" + data;
            System.out.println(msg);

            //create the producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("pkg-input", msg);

            //send data
            produce.send(record);
        }


        //flush and close
        produce.flush();
        produce.close();
    }
}
