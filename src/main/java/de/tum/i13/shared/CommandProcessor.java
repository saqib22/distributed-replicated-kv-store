package de.tum.i13.shared;

import org.json.JSONObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface CommandProcessor {

    String process(String command) throws Exception;

    String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);

    void connectionClosed(InetAddress address);

    void set_metadata(JSONObject metadata);

    void set_server_stopped(boolean status);

    long getTimestamp_last_metadata_updated();

    void setTimestamp_last_metadata_updated(long value);

    JSONObject getMetadata();

    int get_count_nodes_with_ecs_down_detected();

    void set_count_nodes_with_ecs_down_detected(int value);
}
