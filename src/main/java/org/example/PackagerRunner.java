package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.example.consumer.KafkaConsumerHandler;
import org.example.data.DataAbs;
import org.example.data.prepare.PrepareDataFactory;
import org.example.data.prepare.PrepareDataIfc;
import org.example.process.PackagerFactory;
import org.example.process.PackagerHandlerIfc;
import org.example.report.Reporter;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class PackagerRunner {

    private static final Logger log = Logger.getLogger(PackagerRunner.class);

    public static void main(String[] args) throws IOException {

        Properties properties = getConfig(args);

        try(Consumer<String, DataAbs> consumer = KafkaConsumerHandler.createConsumer(properties)) {

            PrepareDataIfc prepareData = PrepareDataFactory.getPrepareDataHandler(properties);
            PackagerHandlerIfc packagerHandler = PackagerFactory.getPackager(properties);
            Reporter reporter = new Reporter();

            while (true) {
                ConsumerRecords<String, DataAbs> records = consumer.poll(Duration.ofMillis(1000));
                Map<String, List<DataAbs>> data = prepareData.prepareData(records);

                data.forEach((key, dataList) -> {
                    packagerHandler.processData(dataList);
                    reporter.report(dataList);
                });

                consumer.commitAsync();
            }
        }
    }

    private static Properties getConfig(String[] args) throws IOException {

        Properties properties = new Properties();
        String propsPath = null;

        if (args.length == 1) {
            if(args[0].equals("local")) {
                PropertyConfigurator.configure("config/log4j.properties");
                propsPath = "config/config-size.props";
            } else {
                propsPath = args[0];
            }
        }

        if(propsPath != null) {
            properties.load(new FileInputStream(propsPath));
            System.out.println("########################  Packager Properties:  ########################");
            properties.keySet().stream()
                    .map(key -> key + ": " + properties.getProperty(key.toString()))
                    .forEach(System.out::println);
        }


        return properties;
    }

    public static Logger getLogger(){
        return log;
    }
}