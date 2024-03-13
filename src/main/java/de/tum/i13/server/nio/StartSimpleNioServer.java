package de.tum.i13.server.nio;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.EchoConnectionBuilder;
import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import org.json.JSONObject;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartSimpleNioServer {

    public static Logger logger;// = Logger.getLogger(StartSimpleNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        logger = setupLogging(cfg.logfile, cfg.logLevel, StartSimpleNioServer.class.getName());
        logger.info("Config: " + cfg.toString());
        CommandProcessor echoLogic = new EchoLogic(cfg, logger);
        echoLogic.set_server_stopped(true);

        EchoConnectionBuilder builder = new EchoConnectionBuilder(cfg.bootstrap.getHostName(), cfg.bootstrap.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection kv server");
                ActiveConnection ecs_connection = null;
                try {
                    ecs_connection = builder.connect();
                    ecs_connection.write("{'job':remove, 'ip': " + cfg.listenaddr + ", 'port': " + cfg.port + "}" + "\r\n");
                    ecs_connection.readline();
                    logger.info("Gracefully removed the KVServer from the cluster");
                    logger.info("Exiting now....");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        new Thread () {
            JSONObject metadata_json;
            @Override
            public void run () {

                logger.info("bootstrapping server via ecs");
                ActiveConnection bootstrap_connection = null;
                try {
                    bootstrap_connection = builder.connect();
                    bootstrap_connection.write("{'job':bootstrap', 'ip': " + cfg.listenaddr + ", 'port': " + cfg.port + "}" + "\r\n");
                    String metadata_string = bootstrap_connection.readline();
                    metadata_json = new JSONObject(metadata_string);
                    echoLogic.set_metadata(metadata_json);
                    bootstrap_connection.close();

                    //start monitoring ECS
                    logger.info("Started ECS Monitor...");
                    Thread monitor_ecs = new MonitorECS(echoLogic, logger, cfg);
                    monitor_ecs.start();

                    return;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            }
        }.start ();

        logger.info("starting server");

        SimpleNioServer sn = new SimpleNioServer(echoLogic);
        sn.bindSockets(cfg.listenaddr, cfg.port);
        sn.start();

    }
}
