package org.example.data.prepare;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.data.DataAbs;

import java.util.List;
import java.util.Map;

public interface PrepareDataIfc {
    Map<String, List<DataAbs>> prepareData(ConsumerRecords<String, DataAbs> records);
}
