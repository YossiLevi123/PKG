package org.example.process;

import org.example.data.DataAbs;

import java.util.Date;
import java.util.Properties;
import java.util.StringJoiner;

import static org.example.utils.PackagerConstants.GROUP_BY_KEY_MD;

public class PackagerHandlerSize extends PackagerHandlerAbs {

    private final double MAX_ZIP_SIZE_MB;
    private long dataSize = 0;

    public PackagerHandlerSize(Properties properties) {
        super(properties);
        this.MAX_ZIP_SIZE_MB = Double.parseDouble(properties.getProperty("packager.max.zip.size.mb", "3"));
    }

    @Override
    protected String getZipFinalMessage(String filePath) {
        return String.format("Successful completion to zip path - %s with %s files, size %sM", filePath, filesCount, getDataSizeInZip());
    }

    @Override
    public boolean isOverLimit() {
        // TODO: estimate zip size
        return getDataSizeInZip() >= MAX_ZIP_SIZE_MB;
    }

    private double getDataSizeInZip() {
        return (dataSize / 1000d / 1000);
    }

    @Override
    public void decreaseDataFromZip(DataAbs dataAbs) {
        super.decreaseDataFromZip(dataAbs);
        this.dataSize -= dataAbs.getData().length();
    }

    @Override
    public void appendData(DataAbs data) {
        super.appendData(data);
        int length = data.getData().length();
        this.dataSize += length;
    }

    @Override
    public void clearData() {
        super.clearData();
        this.dataSize = 0;
    }

    @Override
    public String getZipFileName() {

        StringJoiner fileName = new StringJoiner("_");

        DataAbs dataAbs = this.dataToZip.get(0);

        if(dataAbs.getMetaData().containsKey(GROUP_BY_KEY_MD)) {
            fileName.add(dataAbs.getMetaData().get(GROUP_BY_KEY_MD));
        }

        String uid = dataAbs.getMetaData().get("uid");
        fileName.add(uid);

        long time = new Date().getTime();
        fileName.add(String.valueOf(time));

        return fileName.toString();
    }
}
