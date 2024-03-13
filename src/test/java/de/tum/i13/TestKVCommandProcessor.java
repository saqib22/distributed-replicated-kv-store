package de.tum.i13;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.Config;
import org.junit.jupiter.api.Test;

import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestKVCommandProcessor {

    @Test
    public void correctParsingOfPut() throws Exception {

        KVStore kv = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process("put key hello");

        verify(kv).put("key", "hello");
    }

    @Test
    void setValueTest() throws Exception {
        Config config = new Config();
        config.cache_strategy = "LFU";
        config.cache_size = 10;
        EchoLogic echoLogic = new EchoLogic(config, Logger.getLogger(EchoLogic.class.getName()));
        echoLogic.process("delete key");
        assertEquals("put_success key\r\n", echoLogic.process("put key value"));
        echoLogic.process("delete key");
    }
    @Test
    void getValueTest() throws Exception {
        Config config = new Config();
        config.cache_strategy = "LFU";
        config.cache_size = 10;
        EchoLogic echoLogic = new EchoLogic(config, Logger.getLogger(EchoLogic.class.getName()));
        echoLogic.process("put key value");
        assertEquals("get_success key value\r\n", echoLogic.process("get key"));
        echoLogic.process("delete key");
    }
    @Test
    void updateValueTest() throws Exception {
        Config config = new Config();
        config.cache_strategy = "LFU";
        config.cache_size = 10;
        EchoLogic echoLogic = new EchoLogic(config, Logger.getLogger(EchoLogic.class.getName()));
        echoLogic.process("put key v1");
        assertEquals("put_update key\r\n", echoLogic.process("put key v2"));
        echoLogic.process("delete key");
    }
    @Test
    void getNonExistentValueTest() throws Exception {
        Config config = new Config();
        config.cache_strategy = "LFU";
        config.cache_size = 10;
        EchoLogic echoLogic = new EchoLogic(config, Logger.getLogger(EchoLogic.class.getName()));
        assertEquals("get_error key2\r\n", echoLogic.process("get key2"));
    }

}
