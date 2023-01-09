package org.example.process;

import org.example.PackagerRunner;
import org.example.data.DataAbs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public abstract class PackagerHandlerAbs implements PackagerHandlerIfc {

    protected static final String TEMP_SUFFIX = "_tmp";
    protected final String FILE_EXTENSION;
    protected final String DIRECTORY_PATH;
    protected final String ENTRY_NAME_FIELD;

    protected List<DataAbs> dataToZip = new ArrayList<>();
    protected int filesCount = 0;


    public PackagerHandlerAbs(Properties properties) {
        FILE_EXTENSION = properties.getProperty("packager.file.extension");
        DIRECTORY_PATH = properties.getProperty("packager.directory.path");
        ENTRY_NAME_FIELD = properties.getProperty("packager.entry.name.field");
    }

    @Override
    public void processData(List<DataAbs> dataList) {

        dataList.forEach(data -> {

            appendData(data);

            if (this.isOverLimit()) {
                packageData();
                clearData();
            }
        });

        // The rest
        packageData();
        clearData();
    }

    public void packageData() {

        if (this.dataToZip.isEmpty()) {
            return;
        }

        String zipName = getZipFileName();
        String filePath = DIRECTORY_PATH + File.separator + zipName + ".zip" + TEMP_SUFFIX;

        try (FileOutputStream zipFile = new FileOutputStream(filePath);
             ZipOutputStream zipOutputStream = new ZipOutputStream(zipFile);
             PrintStream ps = new PrintStream(zipOutputStream, false, StandardCharsets.UTF_8)) {

            for (DataAbs data : this.dataToZip) {
                String dataStr = data.getData();
                Map<String, String> metaData = data.getMetaData();

                String fileName = metaData.get(ENTRY_NAME_FIELD);

                ZipEntry zipEntry = new ZipEntry(fileName + '.' + getFileExtension());

                try {
                    zipOutputStream.putNextEntry(zipEntry);
                    ps.print("\ufeff");
                    ps.flush();
                    zipOutputStream.write(dataStr.getBytes(StandardCharsets.UTF_8), 0, dataStr.getBytes(StandardCharsets.UTF_8).length);
                    zipOutputStream.closeEntry();

                } catch (Exception exception) {
                    String errMsg = String.format("packageData() - failed to put entry in zip with error %s", exception.getMessage());
                    PackagerRunner.getLogger().error(errMsg);
                    data.setErrorMsg(errMsg);
                    decreaseDataFromZip(data);
                }
            }

            zipOutputStream.flush();
            zipOutputStream.finish();
        } catch (Exception exception) {
            String errMsg = String.format("packageData() - Failed to zip file %s with error %s", filePath, exception.getMessage());
            PackagerRunner.getLogger().error(errMsg);
            handleZipFailure(errMsg);
        }

        finalizeZip(filePath);
    }

    protected void finalizeZip(String tempFilePath) {
        String finalFilePath = tempFilePath.replace(TEMP_SUFFIX, "");
        try {
            Files.move(new File(tempFilePath).toPath(), new File(finalFilePath).toPath(), StandardCopyOption.REPLACE_EXISTING);
            PackagerRunner.getLogger().info(getZipFinalMessage(finalFilePath));
        } catch (IOException e) {
            String errMsg = String.format("finalizeZip() - Failed to rename zip (%s) with error %s.", tempFilePath, e.getMessage());
            PackagerRunner.getLogger().error(errMsg);
            handleZipFailure(errMsg);
        }
    }

    protected abstract String getZipFinalMessage(String filePath);

    protected String getFileExtension() {
        return this.FILE_EXTENSION;
    }

    protected void clearData() {
        this.dataToZip.clear();
        this.filesCount = 0;
    }

    protected void appendData(DataAbs data) {
        this.dataToZip.add(data);
        this.filesCount++;
    }

    protected void decreaseDataFromZip(DataAbs dataAbs) {
        this.filesCount--;
    }

    protected abstract boolean isOverLimit();

    protected abstract String getZipFileName();

    private void handleZipFailure(String errMsg) {
        this.dataToZip.forEach(data -> {
            data.setErrorMsg(errMsg);
        });
    }
}
