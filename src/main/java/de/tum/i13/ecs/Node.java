package de.tum.i13.ecs;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.EchoConnectionBuilder;

import java.io.IOException;
import java.math.BigInteger;

public class Node {
    public String ip_address;
    public int port;
    public String hash_value;
    public BigInteger hash_integer;
    public String range_min;
    public String range_max;
    public Node successor;
    public Node replica_1;    //replicated data of some node
    public Node replica_2;    //replicated data of some node
    public ActiveConnection connection;

    Node(String ip_address, int port, String hash_value){
        this.ip_address = ip_address;
        this.port = port;
        this.hash_value = hash_value;
        this.hash_integer = new BigInteger(this.hash_value, 16);
        this.replica_1 = null;
        this.replica_2 = null;
    }

    public void set_connection_socket(){
        EchoConnectionBuilder builder = new EchoConnectionBuilder(this.ip_address, this.port);
        try {
            this.connection = builder.connect();
            String confirmation = this.connection.readline();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ActiveConnection getConnection(){
        return this.connection;
    }

    public String get_range_min() {
        return this.range_min;
    }

    public String get_range_max(){
        return this.range_max;
    }

    public Node get_successor(){
        return this.successor;
    }

    public BigInteger get_hash_integer(){
        return this.hash_integer;
    }

    public void set_range_min(String range_min) { this.range_min = range_min; }

    public void set_range_max(String range_max) { this.range_max = range_max; }

    public void set_successor(Node successor) { this.successor = successor; }

    public String get_replica1_range(){
        return this.replica_1.range_min + ":" + this.replica_1.range_max;
    }

    public String get_replica2_range(){
        return this.replica_2.range_min + ":" + this.replica_2.range_max;
    }

    public void set_replica1(Node r1){
        this.replica_1 = r1;
    }

    public void set_replica2(Node r2){
        this.replica_2 = r2;
    }

    public String print_node(){
        StringBuilder node_info = new StringBuilder();
        node_info.append("\n-----------New Node-------------\n");
        node_info.append("IP Address: " + this.ip_address + "\n");
        node_info.append("Port: " + this.port + "\n");
        node_info.append("Hash value: " + this.hash_value + "\n");
        node_info.append("Hash Integer: " + this.get_hash_integer() + "\n");
        node_info.append("Key Range: " + this.get_range_min() + ":" + this.get_range_max() + "\n");
        if (this.get_successor() != null)
            node_info.append("Successor: " + this.get_successor().hash_value + "\n");
        else
            node_info.append("Successor: None\n");
        if (this.replica_1 != null){
            node_info.append("Target Replica 1: " + this.successor.ip_address + ":" + this.successor.port + "\n");
            node_info.append("Host Replica 1 range: " + this.replica_1.range_min + ":" + this.replica_1.range_max + "\n");
        }
        if (this.replica_2 != null){
            node_info.append("Target Replica 2: " + this.successor.successor.ip_address + ":" + this.successor.successor.port + "\n");
            node_info.append("Host Replica 2 range: " + this.replica_2.range_min + ":" + this.replica_2.range_max + "\n");
        }
        node_info.append("----------------------------");
        return node_info.toString();
    }
}