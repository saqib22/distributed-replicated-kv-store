package de.tum.i13;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import de.tum.i13.client.ActiveConnection;
import de.tum.i13.ecs.ConnectionHandleThread;
import de.tum.i13.ecs.HashRing;
import de.tum.i13.ecs.Main;
import de.tum.i13.ecs.MetaData;
import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

/*
We use Junit 5 (or also called Junit Jupiter)
Many online tutorials use Junit 4, the API changed slightly
Userguide: https://junit.org/junit5/docs/current/user-guide/#writing-tests
 */
public class TestExample {

//    @Test
//    public void testDeleteNoSpecChar() throws Exception {
//        Config config = new Config();
//        config.cache_strategy = "FIFO";
//        config.cache_size = 10;
//        EchoLogic echoLogic = new EchoLogic(config);
//        echoLogic.process("get apple");
//        echoLogic.process("put apple orange-@+$");
//        echoLogic.process("get apple");
//        echoLogic.process("put dog one two three");
//        echoLogic.process("get dog");
//        echoLogic.process("delete universe");
//        assertEquals("delete_success apple\r\n", echoLogic.process("delete apple"));
//        echoLogic.process("delete dog");
//        echoLogic.process("delete apple");
//    }
//    @Test
//    public void testPut() throws Exception {
//        String keyToTest = "dog";
//        String valueToTest = "running everywhere";
//        Config config = new Config();
//        config.cache_strategy = "FIFO";
//        config.cache_size = 10;
//        EchoLogic echoLogic = new EchoLogic(config);
//        assertEquals("put_success " + keyToTest + "\r\n", echoLogic.process("put " + keyToTest + " " + valueToTest));
//        //echoLogic.process("delete dog");
//        echoLogic.process("delete " + keyToTest);
//    }
//    @Test
//    public void testDelete() throws Exception {
//        String keyToTest = "apple123apple123";
//        String valueToTest = "orange-@+$";
//        Config config = new Config();
//        config.cache_strategy = "FIFO";
//        config.cache_size = 10;
//        EchoLogic echoLogic = new EchoLogic(config);
//        echoLogic.process("get " + keyToTest);
//        echoLogic.process("put " + keyToTest + " orange-@+$");
//        echoLogic.process("get " + keyToTest);
//        echoLogic.process("put dog one two three");
//        echoLogic.process("get dog");
//        echoLogic.process("delete universe");
//        assertEquals("delete_success " + keyToTest + "\r\n", echoLogic.process("delete " + keyToTest));
//        echoLogic.process("delete dog");
//        echoLogic.process("delete " + keyToTest);
//    }
//    @Test
//    public void testECS() throws IOException {
//        String[] args = new String[]{"-p", "5153", "-a", "127.0.0.1"};
//        Config cfg = parseCommandlineArgs(args);
//        CommandProcessor logic = new EchoLogic(cfg);
//        ServerSocket serverSocket = new ServerSocket();
//        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));
//        assertFalse(serverSocket.isClosed());
//    }
//    @Test
//    public void testRetry() throws Exception {
//        //String[] args = new String[]{"-p", "5153", "-a", "127.0.0.1"};
//        String keyToTest = "dog";
//        String valueToTest = "running everywhere";
//        Config config = new Config();
//        config.cache_strategy = "FIFO";
//        config.cache_size = 10;
//        EchoLogic echoLogic = new EchoLogic(config);
//        assertEquals("server_stopped " + keyToTest + "\r\n", echoLogic.process("put " + keyToTest + " " + valueToTest));
//        //echoLogic.process("delete dog");
//        //echoLogic.process("delete " + keyToTest);
//    }
}
