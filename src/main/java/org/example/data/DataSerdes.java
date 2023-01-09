package org.example.data;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.PackagerRunner;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DataSerdes implements Deserializer<DataAbs> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public DataAbs deserialize(String s, byte[] bytes) {

        DataAbs dataAbs = new DataAbs();

        String msg = new String(bytes, StandardCharsets.UTF_8);
        String[] split = msg.split("\n");

        try {
            String mdStr = split[0];
            String dataStr = split[1];

            dataAbs.setMdStr(mdStr);
            dataAbs.setData(dataStr);

            Map<String, String> metaData = mapper.readValue(msg, Map.class);
            dataAbs.setMetaData(metaData);

        } catch (Exception e) {
            String errMsg = String.format("DataSerdes:deserialize - Failed to deserialize message to metadata + data with error %s", e.getMessage());
            PackagerRunner.getLogger().error(errMsg);
            dataAbs.setErrorMsg(errMsg);
        }

        return dataAbs;
    }
}
