package org.example.report;

import org.example.PackagerRunner;
import org.example.data.DataAbs;

import java.util.List;

public class Reporter {

    public void report(List<DataAbs> dataAbs) {

        ReportMessage reportMessage = new ReportMessage(dataAbs.get(0).getMetaData().get("a1"));
        dataAbs.forEach(data -> {
            reportMessage.inputCount++;
            switch (data.getMessageStatus()) {
                case Failure -> reportMessage.failedCount++;
                case Success -> reportMessage.outputCount++;
            }
        });

        PackagerRunner.getLogger().info(reportMessage);
    }

    static class ReportMessage {
        public final String key;
        public int inputCount = 0;
        public int outputCount = 0;
        public int failedCount = 0;

        ReportMessage(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return "{\n" +
                    "\"key\":" + key +",\n" +
                    "\"inputCount\":" +  inputCount + ",\n" +
                    "\"outputCount\":" +  outputCount + ",\n" +
                    "\"failedCount\":" + failedCount + "\n" +
                    "}";
        }
    }
}
