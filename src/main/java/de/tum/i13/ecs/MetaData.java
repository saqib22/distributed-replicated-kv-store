package de.tum.i13.ecs;

import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class MetaData implements Serializable {
    public static Hashtable<String, Node> metadata;

    MetaData(){
        metadata = new Hashtable<>();
    }

    public Node get_node(String key){
        return metadata.get(key);
    }

    public void put_node(String key, Node node){
        metadata.put(key, node);
    }

    public void delete_node(String key){ metadata.remove(key); }

    public int num_servers(){
        return metadata.size();
    }

    public ArrayList<Node> get_nodes(){
        return new ArrayList<>(metadata.values());
    }

    public JSONObject jsonify_metadata(boolean with_replication){
        JSONObject meta = new JSONObject();
        ArrayList<Node> nodes = get_nodes();
        for (Node node: nodes){
            JSONObject node_attributes = new JSONObject();
            node_attributes.put("ip", node.ip_address);
            node_attributes.put("port", String.valueOf(node.port));
            node_attributes.put("range_min", node.get_range_min());
            node_attributes.put("range_max", node.get_range_max());
            if (node.successor != null)
                node_attributes.put("successor", node.successor.hash_value);
            else
                node_attributes.put("successor", "None");
            if (with_replication && node.replica_1 != null && node.replica_2 != null){
                node_attributes.put("replication", "true");
                JSONObject replication_attributes = new JSONObject();
                replication_attributes.put("replica_host1", node.replica_1.hash_value);
                replication_attributes.put("range_host_replica1", node.get_replica1_range());
                replication_attributes.put("replica_host2", node.replica_2.hash_value);
                replication_attributes.put("range_host_replica2", node.get_replica2_range());
                replication_attributes.put("addr_target_replica1", node.successor.ip_address + ":"
                                            + node.successor.port + ":"
                                            + node.successor.range_min + ":"
                                            + node.successor.range_max);
                replication_attributes.put("addr_target_replica2", node.successor.successor.ip_address + ":"
                                            + node.successor.successor.port + ":"
                                            + node.successor.successor.range_min + ":"
                                            + node.successor.successor.range_max);
                node_attributes.put("replication_attributes", replication_attributes);
            }else{
                node_attributes.put("replication", "false");
            }
            meta.put(node.hash_value.toString(), node_attributes);
        }
        return meta;
    }

    public void initialize(String metadata_json) {
        /*
        * This method re-initialize the ECS's metadata when it
        * is restarted by one of the nodes
        * Input: JSON String with state of the last metadata update
        *        from the ECS
        * */
        JSONObject meta = new JSONObject(metadata_json);
        for (Iterator<String> it = meta.keys(); it.hasNext(); ) {
            String hash_value = it.next();
            JSONObject server = (JSONObject) meta.get(hash_value);
            Node new_node = new Node((String) server.get("ip"),
                    (Integer) server.get("port"),
                    hash_value);
            new_node.set_range_min((String) server.get("range_min"));
            new_node.set_range_max((String) server.get("range_max"));
            new_node.set_connection_socket();
            metadata.put(hash_value, new_node);
        }

        //setting the successor and replicas of the nodes if any.
        for (Node node: get_nodes()){
            JSONObject node_json = (JSONObject) meta.get(node.hash_value);
            if (!node_json.get("successor").equals("None")){
                String successor_hash = (String) node_json.get("successor");
                node.set_successor(metadata.get(successor_hash));
            }else
                node.set_successor(null);

            if ((Boolean) node_json.get("replication")){
                String replica1_hash = (String) node_json.get("replica_host1");
                node.set_replica1(metadata.get(replica1_hash));

                String replica2_hash = (String) node_json.get("replica_host2");
                node.set_replica1(metadata.get(replica1_hash));
            }
        }
    }
}
