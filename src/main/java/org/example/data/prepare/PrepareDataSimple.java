package org.example.data.prepare;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.data.DataAbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrepareDataSimple implements PrepareDataIfc {

    @Override
    public Map<String, List<DataAbs>> prepareData(ConsumerRecords<String, DataAbs> records) {

        Map<String, List<DataAbs>> data = new HashMap<>();
        records.forEach(record -> data.computeIfAbsent("_", s -> new ArrayList<>()).add(record.value()));

        return data;
    }
}
