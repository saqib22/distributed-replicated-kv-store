package de.tum.i13.server.nio;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DockerClientBuilder;
import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.EchoConnectionBuilder;
import de.tum.i13.ecs.Main;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Constants;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MonitorECS extends Thread{
    private CommandProcessor echo;
    private Logger logger;
    private boolean ECS_DOWN;
    private int num_nodes;
    private String host_addr;
    private int host_port;
    private JSONObject metadata;
    private Config config;
    public MonitorECS(CommandProcessor echo, Logger logger, Config config){
        this.echo = echo;
        this.logger = logger;
        this.ECS_DOWN = false;
        this.num_nodes = 0;
        this.host_addr = config.listenaddr;
        this.host_port = config.port;
        this.metadata = null;
        this.config = config;
    }

    @Override
    public void run() {
        while(true){
            try {
                TimeUnit.MILLISECONDS.sleep(100);

                //when the consensus between the nodes is reached that the ECS is infact down
                if (this.ECS_DOWN && echo.get_count_nodes_with_ecs_down_detected() == this.num_nodes){
                    logger.info("Consensus is reached on this node. Checking for the node with highest PID");

                    //Determine the one with the largest hash_value
                    JSONObject target_node = null;
                    for (Iterator<String> it = this.metadata.keys(); it.hasNext(); ) {
                        String key = it.next();

                        JSONObject temp_node = (JSONObject) this.metadata.get(key);
                        if (target_node == null){
                            target_node = temp_node;
                            continue;
                        }

                        BigInteger hash_int = new BigInteger(key, 16);

                        if (hash_int.compareTo(new BigInteger(key, 16)) > 0){
                            target_node = temp_node;
                        }
                    }

                    String node_addr = (String) target_node.get("ip");
                    int node_port = Integer.parseInt((String) target_node.get("port"));

                    if (node_addr.equals(this.host_addr) && node_port == this.host_port){ //re-initialize the ECS
                        logger.info("Intializing the ECS now.....");

                        //start the ECS process
                        ECSJavaProcess.exec(Main.class, Arrays.asList("-ll","OFF", "-m", metadata.toString()));
                        logger.info("The ECS is re-started at this node");

                    }
                    else{ // ignore the ECS initialization if this server is not responsible for the server startup and exit election
                        logger.info("This KVServer is not responsible for ECS initialization. It should be started by " +
                                node_addr + ":" + node_port);
                    }

                    this.ECS_DOWN = false;
                    this.num_nodes = 0;
                    this.echo.set_count_nodes_with_ecs_down_detected(0);
                    this.echo.setTimestamp_last_metadata_updated(0);

                }

                if (!ECS_DOWN && echo.getTimestamp_last_metadata_updated()!=0 &&
                        ((System.nanoTime() - echo.getTimestamp_last_metadata_updated()) / 1000000000) > Constants.ECS_DOWN_TIME){
                    //initiate ecs startup
                    logger.info("ECS failure detected, Starting consensus for ECS boot up...");
                    this.metadata = echo.getMetadata();
                    //Gossip the ecs down message among the active nodes in the cluster
                    for (Iterator<String> it = this.metadata.keys(); it.hasNext(); ) {
                        String key = it.next();
                        JSONObject node = (JSONObject) this.metadata.get(key);

                        String node_addr = (String) node.get("ip");
                        int node_port = Integer.parseInt((String) node.get("port"));

                        if (Objects.equals(node_addr, this.host_addr) && node_port == this.host_port)
                            continue;   //skip sending the message to yourself

                        EchoConnectionBuilder builder = new EchoConnectionBuilder(node_addr, node_port);
                        ActiveConnection connection = builder.connect();
                        connection.readline();

                        connection.write("ecs_down\r\n");
                        connection.readline();
                        connection.close();

                        this.num_nodes += 1;
                    }
                    logger.info("Sent ECS Down SIGNAL to "  + this.num_nodes + " nodes");
                    this.ECS_DOWN = true;
                }

            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
