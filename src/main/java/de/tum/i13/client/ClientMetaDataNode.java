package de.tum.i13.client;

public class ClientMetaDataNode {
    private String iPAddress;
    private String port;
    private String rangeFrom;
    private String rangeTo;

    public ClientMetaDataNode(String iPAddress, String port, String rangeFrom, String rangeTo) {
        this.iPAddress = iPAddress;
        this.port = port;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
    }

    public String getiPAddress() {
        return iPAddress;
    }

    public void setiPAddress(String iPAddress) {
        this.iPAddress = iPAddress;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getRangeFrom() {
        return rangeFrom;
    }

    public void setRangeFrom(String rangeFrom) {
        this.rangeFrom = rangeFrom;
    }

    public String getRangeTo() {
        return rangeTo;
    }

    public void setRangeTo(String rangeTo) {
        this.rangeTo = rangeTo;
    }
}
