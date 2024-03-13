package de.tum.i13.ecs;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Logger;

public class ConnectionHandleThread extends Thread {
    private CommandProcessor cp;
    private Socket kv_store_socket;
    private BufferedReader kv_store_input;
    private PrintWriter kv_store_output;
    public HashRing ring;
    public MetaData metadata;
    public Logger logger;

    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket kv_store_socket, MetaData metaData, Logger logger) {
        this.cp = commandProcessor;
        this.kv_store_socket = kv_store_socket;
        this.metadata = metaData;
        this.ring = new HashRing(this.metadata);
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            this.kv_store_input = new BufferedReader(new InputStreamReader(this.kv_store_socket.getInputStream(), Constants.TELNET_ENCODING));
            this.kv_store_output = new PrintWriter(new OutputStreamWriter(this.kv_store_socket.getOutputStream(), Constants.TELNET_ENCODING));

            JSONObject server_addr = new JSONObject(this.kv_store_input.readLine());

            //handling the shutdown hook of the KVServer
            if (server_addr.get("job").equals("remove")){
                logger.info("Closing the connection to " + server_addr.get("ip") + ":" + server_addr.get("port"));

                if (metadata.get_nodes().size() == 1){
                    this.metadata.delete_node(this.ring.get_md5_hash(server_addr.get("ip")+":"+ server_addr.get("port")));
                    this.kv_store_output.write("kv_server_removed\r\n");
                    this.kv_store_output.flush();
                    return;
                }

                //recalculating and updating the metarata
                Node target_node = this.ring.rebalance_remove_server(server_addr, this.ring.get_md5_hash(server_addr.get("ip")+":"+ server_addr.get("port")));

                //setting write lock on the target node which is to be deleted
                target_node.getConnection().write("server_write_lock" + "\r\n");
                String ack = target_node.getConnection().readline();
                logger.info("Write lock at " + target_node.ip_address + ":" + target_node.port +" => " + ack);

                //deleting the target node noe from the metadata
                this.metadata.delete_node(target_node.hash_value);
                //this.metadata.delete_node(this.ring.get_md5_hash(server_addr.get("ip")+":"+ server_addr.get("port")));

                //updating the metadata so that it accepts the data of the node to be recmoved
                target_node.successor.getConnection().write("metadata_update " + metadata.jsonify_metadata(Main.replication).toString() + "\r\n");
                String ack_update = target_node.successor.getConnection().readline();
                logger.info("Metadata Updated at " + target_node.successor.ip_address + ":" + target_node.successor.port +" => " + ack_update);

                //sending the data from target node to the successor
                target_node.getConnection().write("invoke_repartition " + target_node.successor.ip_address + " " + target_node.successor.port + "\r\n");
                String ack_parition = target_node.getConnection().readline();
                logger.info("Repartitioned the cluster");

                //rebalance the replicas if there is replication enabled in the cluster
//                if (Main.replication){
//                    target_node.successor.replica_1 = target_node.replica_1;
//                    target_node.successor.replica_2 = target_node.replica_2;
//
//                    //delete replica_1 data from target node's successor
//
//
//                    //send replica1 of target node to the successor
//                    target_node.successor.getConnection().write("send_replica_data " +
//                            target_node.successor.ip_address + " " +
//                            target_node.successor.port + " " +
//                            target_node.successor.replica_1.range_min + " " +
//                            target_node.successor.replica_1.range_max + "\r\n"
//                    );
//                    target_node.successor.getConnection().readline();
//
//                    //rebalance the second replicated paritions among the nodes
//                    target_node.successor.getConnection().write("send_replica_data " +
//                            target_node.ip_address + " " +
//                            target_node.port + " " +
//                            target_node.successor.replica_2.range_min + " " +
//                            target_node.successor.replica_2.range_max + "\r\n"
//                    );
//                    target_node.successor.getConnection().readline();
//
//
//
//                }

                //sending metadata update to all nodes in the system
                if (ack_parition.equals("repartition_success")){
                    logger.info("Sending metadata update to all nodes");
                    for (Node node: metadata.get_nodes()){
                        node.getConnection().write("metadata_update " + metadata.jsonify_metadata(Main.replication).toString() + "\r\n");
                        node.getConnection().readline();
                    }
                }

                for (Node node: metadata.get_nodes()){
                    logger.info(node.print_node());
                }

                this.kv_store_output.write("kv_server_removed\r\n");
                this.kv_store_output.flush();

                return;
            }

            String hash_value = this.ring.get_md5_hash(server_addr.get("ip")+":"+ server_addr.get("port"));
            synchronized (metadata){
                Node new_node = ring.rebalance_add_server(server_addr, hash_value);
                //Sending metadata to the new KVStore
                this.kv_store_output.write(metadata.jsonify_metadata(Main.replication).toString() + "\r\n");
                this.kv_store_output.flush();

                while(true){
                    try{
                        new_node.set_connection_socket();
                        logger.info("Initialized KVServer at " + server_addr.get("ip") + ":" + server_addr.get("port"));
                        break;
                    }
                    catch (Exception e){
                        logger.info("Trying to reconnect with the New KVServer....");
                        continue;
                    }
                }
                if (new_node.get_successor() == null){
                    for (Node node: metadata.get_nodes()){
                        logger.info(node.print_node());
                    }
                    return;
                }

                //set write lock at the successor
                new_node.successor.getConnection().write("server_write_lock" + "\r\n");
                String ack = new_node.successor.getConnection().readline();
                logger.info("Write lock at " + new_node.successor.ip_address + ":" + new_node.successor.port +" => " + ack);

                //metadata update at the successor
                new_node.successor.getConnection().write("metadata_update " + metadata.jsonify_metadata(Main.replication).toString() + "\r\n");
                String ack_update = new_node.successor.getConnection().readline();
                logger.info("Metadata Updated at " + new_node.successor.ip_address + ":" + new_node.successor.port +" => " + ack_update);

                //starting to repartition the KVServers
                new_node.successor.getConnection().write("invoke_repartition " + new_node.ip_address + " " + new_node.port + "\r\n");
                String ack_partition = new_node.successor.getConnection().readline();
                logger.info("Repartitioned the cluster");

                //check for replication
                if (metadata.num_servers() > 2){
                    //base case to initialize the replication when num_servers is 3
                    logger.info("Starting replication....");
                    if (metadata.num_servers() == 3){
                        logger.info("Bootstrap replication....");
                        Main.replication = true;
                        //initializing the replicas
                        for (Node node: metadata.get_nodes()){
//                            node.replica_1 = node.successor;
//                            node.replica_2 = node.successor.successor;
                            node.successor.replica_2 = node;
                            node.successor.replica_1 = node.successor.successor;
                        }

                        //initializing the replication on all the nodes
                        for (Node node: metadata.get_nodes()){
                            //Initializing the replication on the nodes
                            node.getConnection().write("init_replication "
                                    + node.get_replica1_range()+ " "
                                    + node.get_replica2_range() + " "
                                    + node.successor.ip_address + ":" + node.successor.port + " "
                                    + node.successor.successor.ip_address + ":" + node.successor.successor.port + "\r\n");
                            node.getConnection().readline();
                        }
                    }
                    else {
                        logger.info("Configuring replication...");
                        //setting write on the successor of successor also the soon to be replica 2 of new node
                        //this is to avoid the predecessor of the new node to send the information to its replica 2
                        // which will be redundant as the new node will become one of its replicated nodes
                        new_node.successor.successor.getConnection().write("server_write_lock" + "\r\n");
                        logger.info("Write lock at " + new_node.successor.successor.ip_address + ":" + new_node.successor.successor.port + " => " +
                                new_node.successor.successor.getConnection().readline());

                        //rebalance the first replicated paritions among the nodes
                        new_node.successor.getConnection().write("send_replica_data " +
                                new_node.ip_address + " " +
                                new_node.port + " " +
                                new_node.successor.replica_1.range_min + " " +
                                new_node.successor.replica_1.range_max + "\r\n"
                        );
                        new_node.successor.getConnection().readline();

                        //rebalance the second replicated paritions among the nodes
                        new_node.successor.getConnection().write("send_replica_data " +
                                new_node.ip_address + " " +
                                new_node.port + " " +
                                new_node.successor.replica_2.range_min + " " +
                                new_node.successor.replica_2.range_max + "\r\n"
                        );
                        new_node.successor.getConnection().readline();

                        //reconfigure the whole replication system
                        new_node.replica_1 = new_node.successor.replica_1;
                        new_node.replica_2 = new_node.successor.replica_2;
                        new_node.successor.replica_1 = new_node.replica_2;
                        new_node.successor.replica_2 = new_node;

                        //propagate the host range changes across the ring
                        for (Node node: metadata.get_nodes()){
                            node.successor.replica_2 = node;
                            node.successor.replica_1 = node.replica_2;
                        }

                        //Initializing the replicas on the KVServer
                        new_node.getConnection().write("init_replication "
                                + new_node.get_replica1_range() + " "
                                + new_node.get_replica2_range() + " "
                                + new_node.successor.ip_address + ":" + new_node.successor.port + " "
                                + new_node.successor.successor.ip_address + ":" + new_node.successor.successor.port + "\r\n");
                        new_node.getConnection().readline();
                    }

                }

                //sending metadata update to all nodes in the system
                if (ack_partition.equals("repartition_success")){
                    logger.info("Sending metadata update to all nodes");
                    for (Node node: metadata.get_nodes()){
                        node.getConnection().write("metadata_update " + metadata.jsonify_metadata(Main.replication).toString() + "\r\n");
                        node.getConnection().readline();
                    }
                }

                //Removing the lock on new node successor's successor
                if (Main.replication){
                    new_node.successor.successor.getConnection().write("server_remove_write_lock\r\n");
                    new_node.successor.successor.getConnection().readline();
                }

                //Removing the server write lock and deleting the repartitioned data from the KVServer
                new_node.successor.getConnection().write("server_remove_write_lock\r\n");
                String ack_write_lock_removed = new_node.successor.getConnection().readline();
                if (ack_write_lock_removed.equals("write_lock_removed"))
                    logger.info("Removed write lock from the successor and delete the repartitioned kv pairs");

                //remove the server_stopped signal from the new node
                new_node.getConnection().write("server_start\r\n");
                new_node.getConnection().readline();

                //log all the active nodes in the circle
                for (Node node: metadata.get_nodes()){
                    logger.info(node.print_node());
                }

            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
