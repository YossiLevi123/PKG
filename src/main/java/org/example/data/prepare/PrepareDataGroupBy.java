package org.example.data.prepare;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.data.DataAbs;

import java.util.*;

import static org.example.utils.PackagerConstants.GROUP_BY_KEY_MD;

public class PrepareDataGroupBy implements PrepareDataIfc {

    private final List<String> keyFieldsFromMD;

    public PrepareDataGroupBy(Properties properties) {
        this.keyFieldsFromMD = Arrays.stream(properties.getProperty("prepare.data.group.by.key.fields", "")
                                            .split(","))
                                            .toList();
    }

    @Override
    public Map<String, List<DataAbs>> prepareData(ConsumerRecords<String, DataAbs> records) {

        Map<String, List<DataAbs>> data = new HashMap<>();

        records.forEach(record -> {
            DataAbs value = record.value();

            if(value.isAlive()) {
                String key = getAndSetKey(value.getMetaData());

                if (!data.containsKey(key)) {
                    data.put(key, new ArrayList<>());
                }

                data.get(key).add(value);
            }
        });

        return data;
    }

    private String getAndSetKey(Map<String, String> metaData) {

        StringJoiner key = new StringJoiner("_");
        this.keyFieldsFromMD.forEach(keyName -> key.add(metaData.get(keyName)));
        metaData.put(GROUP_BY_KEY_MD, key.toString());

        return key.toString();
    }

}
