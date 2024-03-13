package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private KVStore kvStore;

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public String process(String command) {
        //TODO
        //Parse message "put message", call kvstore.put
        try {
            this.kvStore.put("key", "hello");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        //TODO

        return null;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        //TODO

    }

    @Override
    public void set_metadata(JSONObject metadata) {}

    @Override
    public void set_server_stopped(boolean status) {}

    @Override
    public long getTimestamp_last_metadata_updated() {
        return 0;
    }

    @Override
    public void setTimestamp_last_metadata_updated(long value) {

    }

    @Override
    public JSONObject getMetadata() {
        return null;
    }

    @Override
    public int get_count_nodes_with_ecs_down_detected() {
        return 0;
    }

    @Override
    public void set_count_nodes_with_ecs_down_detected(int value) {}


}
