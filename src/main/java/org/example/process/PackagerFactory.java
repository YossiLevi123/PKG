package org.example.process;

import java.util.Properties;

public class PackagerFactory {

    public static PackagerHandlerIfc getPackager(Properties properties) {
        String packagerType = properties.getProperty("packager.type", "size");
        PackagerHandlerEnum packagerHandlerType = PackagerHandlerEnum.valueOf(packagerType);

        return switch (packagerHandlerType) {
            case COUNT -> new PackagerHandlerCount(properties);
            case SIZE -> new PackagerHandlerSize(properties);
        };
    }

}
