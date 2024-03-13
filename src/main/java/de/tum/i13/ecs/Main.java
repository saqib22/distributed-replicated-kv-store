package de.tum.i13.ecs;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

    public static boolean replication = false;

    public static Logger logger; //= Logger.getLogger(Main.class.getName());
    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        logger = setupLogging(cfg.logfile, cfg.logLevel, Main.class.getName());

        final ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });


        //bind to localhost only
        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));
        logger.info("The ECS server is up and listening on "+ cfg.listenaddr + ":" + cfg.port + " !");

        //Replace with your Key value server logic.
        // If you use multithreading you need locking
        CommandProcessor logic = new EchoLogic(cfg, logger);

        MetaData metaData = new MetaData();

        //in case the ECS server is restarted as the result of leader election
        if(!cfg.metadata_json.equals("null")){
            logger.info("*******************************");
            logger.info("Restarting the ECS server...");
            metaData.initialize(cfg.metadata_json);
            logger.info("The ECS server is re-initialized");
        }
        //Sending heartbeats to the storage servers
        new Thread () {
            JSONObject metadata_json;
            @Override
            public void run () {
                while(true){
                    try {
                        TimeUnit.SECONDS.sleep(1);
                        if (metaData.get_nodes().size() == 0){
                            continue;
                        }
                        logger.info("Sending heartbeat to the storage servers");
                        for(Node node: metaData.get_nodes()){
                            InetAddress address = InetAddress.getByName(node.ip_address);
                            if (address.isReachable(700)){
                                node.getConnection().write("metadata_update " + metaData.jsonify_metadata(replication).toString() + "\r\n");
                                node.getConnection().readline();
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start ();


        while (true) {
            Socket clientSocket = serverSocket.accept();
            logger.info("Got new server connection from " + clientSocket.getInetAddress().toString() + " at port: " + clientSocket.getPort());

            //When we accept a connection, we start a new Thread for this connection
            Thread th = new ConnectionHandleThread(logic, clientSocket, metaData, logger);
            th.start();
        }
    }
}
