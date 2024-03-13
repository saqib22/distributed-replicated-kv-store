package de.tum.i13.ecs;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class HashRing {
    public MessageDigest md;
    public MetaData metaData;

    public HashRing(MetaData metaData){
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.metaData = metaData;
    }

    public String get_md5_hash(String value){
        this.md.update(value.getBytes());
        byte[] digest = this.md.digest();
        return DatatypeConverter.printHexBinary(digest);
    }

    public Node rebalance_remove_server(JSONObject server_addr, String hash_value){
        Node target_node = metaData.get_node(hash_value);
        ArrayList<Node> nodes = metaData.get_nodes();
        nodes.sort((o1, o2) -> o1.get_hash_integer().compareTo(o2.get_hash_integer()));

        int pred_index = nodes.indexOf(target_node) - 1;
        if (pred_index < 0){ // wrap around the ring
            pred_index = nodes.size() - 1;
        }

        nodes.get(pred_index).set_successor(target_node.get_successor());
        target_node.get_successor().set_range_min(target_node./*get_successor().*/get_range_min());

        return target_node;
        //return target_node.get_successor();
    }

    public Node rebalance_add_server(JSONObject server_addr, String hash_value){
        /*
        * Implementing consistent hashing. This function recalculates
        * the ring and assigns new ranges to the nodes.
        * */
        Node new_node = new Node(
                (String) server_addr.get("ip"),
                (Integer) server_addr.get("port"),
                hash_value
        );
        if (metaData.num_servers() == 0){
            new_node.set_range_min(hash_value);
            new_node.set_range_max(hash_value);
            new_node.set_successor(null);
            metaData.put_node(hash_value, new_node);
            return new_node;
        }
        else{
            metaData.put_node(hash_value, new_node);
            ArrayList<Node> nodes = metaData.get_nodes();
            nodes.sort((o1, o2) -> o1.get_hash_integer().compareTo(o2.get_hash_integer()));

            int pred_index = nodes.indexOf(new_node) - 1;
            int succ_index = nodes.indexOf(new_node) + 1;
            if (pred_index < 0){ // wrap around the ring
                pred_index = nodes.size() - 1;
            }
            if (succ_index >= nodes.size()){ // wrap around the ring
                succ_index = 0;
            }

            //calculating new ranges for the new node
            String range_min = nodes.get(pred_index).range_max;
            String range_max = nodes.get(nodes.indexOf(new_node)).hash_value;
            nodes.get(nodes.indexOf(new_node)).set_range_max(range_max);
            nodes.get(nodes.indexOf(new_node)).set_range_min(range_min);
            nodes.get(pred_index).set_successor(new_node);
            //recalculate the ranges for the successor node
            nodes.get(succ_index).set_range_min(range_max);

            new_node.set_successor(nodes.get(succ_index));

            return new_node;
        }
    }
}
