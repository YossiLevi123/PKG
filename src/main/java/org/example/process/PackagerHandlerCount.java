package org.example.process;

import org.example.data.DataAbs;

import java.util.Properties;
import java.util.StringJoiner;

import static org.example.utils.PackagerConstants.GROUP_BY_KEY_MD;

public class PackagerHandlerCount extends PackagerHandlerAbs {

    private final int MAX_FILES_IN_ZIP;

    public PackagerHandlerCount(Properties properties) {
        super(properties);
        MAX_FILES_IN_ZIP = Integer.parseInt(properties.getProperty("packager.max.files.in.zip", "1000"));
    }

    @Override
    protected String getZipFinalMessage(String filePath) {
        return String.format("Successful completion to zip path - %s with %s files", filePath, filesCount);
    }

    @Override
    public boolean isOverLimit() {
        return this.filesCount >= MAX_FILES_IN_ZIP;
    }


    @Override
    public String getZipFileName() {

        StringJoiner fileName = new StringJoiner("_");

        DataAbs dataAbs = this.dataToZip.get(0);

        if(dataAbs.getMetaData().containsKey(GROUP_BY_KEY_MD)) {
            fileName.add(dataAbs.getMetaData().get(GROUP_BY_KEY_MD));
        }

        fileName.add(String.valueOf(this.filesCount));

        String uid = dataAbs.getMetaData().get("uid");
        fileName.add(uid);

        return fileName.toString();
    }
}
