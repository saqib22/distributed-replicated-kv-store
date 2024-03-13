package de.tum.i13.client;

import de.tum.i13.ecs.Node;

import java.util.ArrayList;
import java.util.HashMap;

public class ClientMetaData {
    private HashMap<String, ClientMetaDataNode> clientMetaDataMap;

    public ClientMetaData(){
        this.clientMetaDataMap = new HashMap<>();
    }

    public void put(String ipPort, String rangeFrom, String rangeTo){
        clientMetaDataMap.put(ipPort, new ClientMetaDataNode(ipPort.split(":")[0], ipPort.split(":")[1], rangeFrom, rangeTo));
    }

    public ArrayList<ClientMetaDataNode> get_nodes(){
        return new ArrayList<>(clientMetaDataMap.values());
    }
}
