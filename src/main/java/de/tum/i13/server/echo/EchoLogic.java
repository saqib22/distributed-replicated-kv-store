package de.tum.i13.server.echo;

import de.tum.i13.StorageEngine.Entry;
import de.tum.i13.caching.Cache;
import de.tum.i13.caching.CacheManager;
import de.tum.i13.client.ActiveConnection;
import de.tum.i13.client.EchoConnectionBuilder;
import de.tum.i13.server.kv.KVDiskStorage;
import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Constants;
import io.netty.util.Timeout;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class EchoLogic implements CommandProcessor {
    public static Logger logger;
    public Config config;
    public Cache cache;
    private JSONObject metadata;
    public KVStore kvStore;
    private boolean write_lock;
    private String server_hash_value;
    public MessageDigest md;
    private String range_min;   //minimum range for the coordinator data for which this node is responsible
    private String range_max;  //maximum range for the coordinator data for which this node is responsible
    private boolean replication;
    private String range_host_replica1;
    private String range_host_replica2;
    private static String target_replica1_addr;
    private static String target_replica2_addr;
    private WAL wal;
    public static boolean server_stopped;
    private long timestamp_last_metadata_updated;
    private int count_nodes_with_ecs_down_detection;
    private boolean SDConsistency;
    public EchoLogic(Config config, Logger log) throws IOException {
        server_stopped = true;
        this.config = config;
        this.replication = false;
        this.write_lock = false;
        this.timestamp_last_metadata_updated = 0;
        this.kvStore = new KVDiskStorage();
        this.wal = new WAL();
        this.count_nodes_with_ecs_down_detection = 0;
        this.cache = new CacheManager(config.cache_strategy, config.cache_size).getCache();
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.server_hash_value = get_md5_hash(config.listenaddr + ":" + config.port);
        logger = log;
        this.SDConsistency = config.SDConsistency;
    }
    public boolean isSDConsistency() {
        return SDConsistency;
    }

    public void setSDConsistency(boolean SDConsistency) {
        this.SDConsistency = SDConsistency;
    }

    public String process(String command) throws Exception {
        logger.info("received command: " + command.trim());

        command = command.replaceAll("\\n", "").replaceAll("\\r", "");
        String[] arguments = command.split("\\s+", 5);

        switch (arguments[0]){
            case "put":{
                arguments = command.split("\\s+", 3);
                if (!this.write_lock){
                    String key_hash = get_md5_hash(arguments[1]);
                    if (in_range(key_hash, this.range_min, this.range_max)) {
                        if(this.SDConsistency){
                            logger.info("Sending data to replicas.");
                            this.wal.wal_process("add", "", command);
                            TimeUnit.MILLISECONDS.sleep(300);
                            logger.info("Data processing done in replicas. Sending data to cache and kvstore.");
                            this.cache.put(key_hash, arguments[1] + ":" + arguments[2]);
                            KVMessage message = this.kvStore.put(key_hash, arguments[1] + ":" + arguments[2]);
                            logger.info("Data processing done at replicas. Returning status.");
                            return message.getStatus().name().toLowerCase(Locale.ROOT) + " " + arguments[1] + "\r\n";
                        }
                        else {
                            this.cache.put(key_hash, arguments[1] + ":" + arguments[2]);
                            KVMessage message = this.kvStore.put(key_hash, arguments[1] + ":" + arguments[2]);
                            this.wal.wal_process("add", "", command);
                            return message.getStatus().name().toLowerCase(Locale.ROOT) + " " + arguments[1] + "\r\n";
                        }
                    }
                    else
                        return "server_not_responsible\r\n";
                }
                else
                    return "server_write_lock\r\n";
            }

            case "put_replica":{
                arguments = command.split("\\s+", 3);
                if (!this.write_lock){
                    String key_hash = get_md5_hash(arguments[1]);

                    this.cache.put(key_hash, arguments[1] + ":" + arguments[2]);
                    KVMessage message = this.kvStore.put(key_hash, arguments[1] + ":" + arguments[2]);

                    logger.info("Added kv pair with " + arguments[1] + ":" + arguments[2]);
                    return message.getStatus().name().toLowerCase(Locale.ROOT) + " " + arguments[1] + "\r\n";
                }
                else
                    return "server_write_lock\r\n";
            }

            case "get":{
                arguments = command.split("\\s+", 3);
                String key_hash = get_md5_hash(arguments[1]);
                if (!in_range(key_hash, this.range_min, this.range_max)
                        && !(this.replication && in_range(key_hash, this.range_host_replica1.split(":")[0], this.range_host_replica1.split(":")[1]))
                        && !(this.replication && in_range(key_hash, this.range_host_replica2.split(":")[0], this.range_host_replica2.split(":")[1]))
                ) {
                    return "server_not_responsible\r\n";
                }
                else if(this.cache.getStatus(key_hash) == Cache.CacheTemp.CACHE_MISS){
                    //Check KVStore if it has the KV pair
                    KVMessage query_kv = this.kvStore.get(key_hash);
                    if(query_kv.getStatus() == KVMessage.StatusType.GET_SUCCESS){
                        this.cache.put(key_hash, arguments[1]+":"+query_kv.getValue());
                        return KVMessage.StatusType.GET_SUCCESS.name().toLowerCase(Locale.ROOT) + " " + arguments[1] + " " + query_kv.getValue().split(":")[1] + "\r\n";
                    }
                    // if the value cannot be found in the KV store as well
                    else
                        return KVMessage.StatusType.GET_ERROR.name().toLowerCase(Locale.ROOT) + " " + arguments[1]  + "\r\n";
                }
                else
                    return KVMessage.StatusType.GET_SUCCESS.name().toLowerCase(Locale.ROOT) + " " + arguments[1] + " " + this.cache.get(key_hash).split(":")[1] + "\r\n";
            }
            case "delete":{
                // since we have the functionality of deleting this element when the value is null
                // extending this to a separate command
                if (!this.write_lock){
                    String key_hash = get_md5_hash(arguments[1]);
                    this.cache.put(key_hash, "null");
                    KVMessage message = this.kvStore.put(key_hash, "null");
                    return message.getStatus().name().toLowerCase(Locale.ROOT) + " " + arguments[1] + "\r\n";
                }
                else
                    return "server_write_lock\r\n";
            }

            case "server_write_lock":{
                logger.info("Write lock on the KVServer initiated");
                this.write_lock = true;
                return "write_lock_success\r\n";
            }

            case "server_remove_write_lock":{
                logger.info("Removing the write lock on the KVServer");
                this.write_lock = false;
                return "write_lock_removed\r\n";
            }

            case "keyrange_read": {
                JSONObject value;
                String range;
                StringBuilder toReturn = new StringBuilder("keyrange_read_success ");
                for (String key : this.metadata.keySet()) {
                    value = (JSONObject) this.metadata.get(key);
                    range = value.get("range_min") + "," + value.get("range_max") + "," + value.get("ip") + ":" + value.get("port") + ";";
                    toReturn.append(range);
                    if (Boolean.parseBoolean((String) value.get("replication"))){
                        JSONObject replication_attributes = (JSONObject) value.get("replication_attributes");
                        String[] r1 = replication_attributes.get("addr_target_replica1").toString().split(":");
                        String[] r2 = replication_attributes.get("addr_target_replica2").toString().split(":");

                        String range_r1 = value.get("range_min") + "," + value.get("range_max") + "," + r1[0] + ":" + r1[1] + ";";
                        toReturn.append(range_r1);
                        String range_r2 = value.get("range_min") + "," + value.get("range_max") + "," + r2[0] + ":" + r2[1] + ";";
                        toReturn.append(range_r2);
                    }
                }
                toReturn.append("\r\n");
                return toReturn.toString();
            }
            case "metadata_update":{
                if(arguments.length == 2){
                    this.metadata = new JSONObject(arguments[1]);
                    JSONObject server = (JSONObject) this.metadata.get(this.server_hash_value);
                    this.range_min = (String) server.get("range_min");
                    this.range_max = (String) server.get("range_max");

                    if (Boolean.parseBoolean((String) server.get("replication"))){
                        JSONObject replication_attributes = (JSONObject) server.get("replication_attributes");
                        this.range_host_replica1 = (String) replication_attributes.get("range_host_replica1");
                        this.range_host_replica2 = (String) replication_attributes.get("range_host_replica2");
                        target_replica1_addr = (String) replication_attributes.get("addr_target_replica1");
                        target_replica2_addr = (String) replication_attributes.get("addr_target_replica2");
                    }
                    //get the time when metadata is updated which is used to detect ECS failure
                    // and initiate leader election
                    this.timestamp_last_metadata_updated = System.nanoTime();

                    return "metadata_update_success\r\n";
                }else
                    return "metadata_update_failed\r\n";
            }

            case "keyrange":{
                JSONObject value;
                String range;
                StringBuilder toReturn = new StringBuilder("keyrange_success ");
                for (String key : this.metadata.keySet()){
                    value = (JSONObject) this.metadata.get(key);
                    range = value.get("range_min") + "," + value.get("range_max") + "," + value.get("ip") + ":" + value.get("port") + ";";
                    toReturn.append(range);
                }
                toReturn.append("\r\n");
                return toReturn.toString();
            }

            case "init_replication":{
                if(arguments.length == 5){
                    logger.info("Starting the replication on this node");
                    this.replication = true;
                    this.range_host_replica1 = arguments[1];
                    this.range_host_replica2 = arguments[2];
                    target_replica1_addr = arguments[3];
                    target_replica2_addr = arguments[4];

                    Thread sync_replicas = new SyncReplicas(this.wal, logger);
                    sync_replicas.start();

                    return "replication_initialized\r\n";
                }
                return "Error while initializing replication!\r\n";
            }

            case "server_start":{
                this.set_server_stopped(false);
                logger.info("The server is started to serve requests");
                return "server_started\r\n";
            }

            case "send_replica_data":{
                if (arguments.length == 5){
                    logger.info("Sending replica partition to " + arguments[1] + ":" + arguments[2]);

                    EchoConnectionBuilder builder = new EchoConnectionBuilder(arguments[1], Integer.parseInt(arguments[2]));
                    ActiveConnection connection = builder.connect();
                    Iterator<Entry<String, String>> query = this.kvStore.get_entries(arguments[3], arguments[4]);
                    int num_records = 0;
                    while(query.hasNext()){
                        Entry<String, String> kv = query.next();
                        connection.write("put " + kv.key() + " " + kv.value() + "\r\n");
                        String ack = connection.readline();
                        num_records++;
                    }
                    logger.info("Sent " + num_records + " kv pairs to " + arguments[1] + ":" + arguments[2]);
                    connection.close();

                    return "sent_replica\r\n";
                }
                return "sending_replica_failed\r\n";
            }

            case "delete_replica":{
                if (arguments.length == 3){
                    Iterator<Entry<String, String>> query = this.kvStore.get_entries(arguments[1], arguments[2]);
                    while(query.hasNext()){
                        Entry<String, String> kv = query.next();
                        String key_hash = get_md5_hash(kv.key());
                        this.cache.put(key_hash, "null");
                        this.kvStore.put(key_hash, "null");
                    }

                    return "replica_deleted\r\n";
                }

                return "error: deleting replica\r\n";
            }

            case "invoke_repartition":{
                if (arguments.length == 3){
                    logger.info("starting to repartition");

                    JSONObject server = (JSONObject) this.metadata.get(this.server_hash_value);
                    String r_min = this.range_min;
                    String r_max = (String) server.get("range_min");

                    logger.info("Connecting to the responsible server for data repartitioning");
                    EchoConnectionBuilder builder = new EchoConnectionBuilder(arguments[1], Integer.parseInt(arguments[2]));
                    ActiveConnection connection = builder.connect();
                    Iterator<Entry<String, String>> query = this.kvStore.get_entries(r_min, r_max);
                    int num_records = 0;
                    while(query.hasNext()){
                        Entry<String, String> kv = query.next();
                        connection.write("put " + kv.key() + " " + kv.value() + "\r\n");
                        String ack = connection.readline();
                        num_records++;
                    }
                    logger.info("Sent " + num_records + " kv pairs to " + arguments[1] + ":" + arguments[2]);
                    connection.close();

                    this.range_min = (String) server.get("range_min");
                    this.range_max = (String) server.get("range_max");

                    //deleting the already the sent data to another node from this node
                    logger.info("Starting to delete the repartitioned data");
                    Iterator<Entry<String, String>> q = this.kvStore.get_entries(r_min, r_max);
                    while(q.hasNext()){
                        Entry kv = q.next();
                        this.kvStore.put((String) kv.key(), "null");
                    }

                    return "repartition_success\r\n";
                }
                return "repartitioning_failed\r\n";
            }

            case "ecs_down":{
                this.count_nodes_with_ecs_down_detection += 1;
                logger.info("ECS Down SIGNAL Received !");
                return "ecs_down_acknowledged\r\n";
            }

            default: {
                return "error unknown command\r\n";
            }
        }
    }

    public String get_md5_hash(String value){
        this.md.update(value.getBytes());
        byte[] digest = this.md.digest();
        return DatatypeConverter.printHexBinary(digest);
    }

    public boolean in_range(String hash, String range_min, String range_max){
        BigInteger r_min = new BigInteger(range_min, 16);
        BigInteger r_max = new BigInteger(range_max, 16);

        if (r_max.equals(r_min)){ return true; }
        BigInteger temp = new BigInteger(hash, 16);

        if (r_min.compareTo(r_max) > 0){
            if (temp.compareTo(r_min) > 0 && temp.compareTo(new BigInteger(Constants.MAX_RING_RANGE, 16)) < 0){
                return true;
            }
            return temp.compareTo(new BigInteger(Constants.MIN_RING_RANGE, 16)) > 0 && temp.compareTo(r_max) < 0;
        }
        else {
            return temp.compareTo(r_max) < 0 && temp.compareTo(r_min) > 0;
        }
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());
        return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        logger.info("connection closed: " + remoteAddress.toString());
    }

    @Override
    public void set_metadata(JSONObject metadata){
        this.metadata = metadata;
        logger.info("Intializing the metadata of the KVServer");
        //initializing server parameters
        JSONObject server = (JSONObject) this.metadata.get(this.server_hash_value);
        this.range_min = (String) server.get("range_min");
        this.range_max = (String) server.get("range_max");
    }

    @Override
    public void set_server_stopped(boolean status){
        server_stopped = status;
    }

    public static ActiveConnection get_target_replica1_connection() throws IOException {
        String[] address = target_replica1_addr.split(":");
        EchoConnectionBuilder builder = new EchoConnectionBuilder(address[0], Integer.parseInt(address[1]));
        ActiveConnection connection = builder.connect();
        String confirmation = connection.readline();
        return connection;
    }
    public static ActiveConnection get_target_replica2_connection() throws IOException {
        String[] address = target_replica2_addr.split(":");
        EchoConnectionBuilder builder = new EchoConnectionBuilder(address[0], Integer.parseInt(address[1]));
        ActiveConnection connection = builder.connect();
        String confirmation = connection.readline();
        return connection;
    }

    @Override
    public long getTimestamp_last_metadata_updated(){
        return this.timestamp_last_metadata_updated;
    }

    @Override
    public void setTimestamp_last_metadata_updated(long value) {
        this.timestamp_last_metadata_updated = value;
    }

    @Override
    public JSONObject getMetadata() {
        return this.metadata;
    }

    @Override
    public int get_count_nodes_with_ecs_down_detected() {
        return this.count_nodes_with_ecs_down_detection;
    }

    @Override
    public void set_count_nodes_with_ecs_down_detected(int value) {
        this.count_nodes_with_ecs_down_detection = value;
    }


}
