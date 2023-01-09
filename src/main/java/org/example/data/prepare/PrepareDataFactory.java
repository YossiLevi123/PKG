package org.example.data.prepare;

import java.util.Properties;

public class PrepareDataFactory {

    public static PrepareDataIfc getPrepareDataHandler(Properties properties) {

        String prepareDataType = properties.getProperty("prepare.data.type", "simple");
        PrepareDataEnum prepareDataEnum = PrepareDataEnum.valueOf(prepareDataType);

        return switch (prepareDataEnum) {
            case SIMPLE ->  new PrepareDataSimple();
            case GROUP_BY_KEY -> new PrepareDataGroupBy(properties);
        };
    }
}
