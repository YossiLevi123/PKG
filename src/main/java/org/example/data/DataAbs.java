package org.example.data;

import org.example.report.MessageStatus;

import java.util.Map;

public class DataAbs {
    private String mdStr;
    private Map<String, String> metaData;
    private String data;
    private MessageStatus messageStatus = MessageStatus.Success;
    private String errorMsg;

    public String getMdStr() {
        return mdStr;
    }

    public void setMdStr(String mdStr) {
        this.mdStr = mdStr;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isAlive() {
        return messageStatus.equals(MessageStatus.Success);
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.messageStatus = MessageStatus.Failure;
        this.errorMsg = errorMsg;
    }

    public MessageStatus getMessageStatus() {
        return messageStatus;
    }
}
