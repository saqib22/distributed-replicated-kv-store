package de.tum.i13.server.kv;

public class KVResponse implements KVMessage{

    private String key;
    private String value;
    private final StatusType status;

    public KVResponse(String key, String value, StatusType status){
        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }

}
