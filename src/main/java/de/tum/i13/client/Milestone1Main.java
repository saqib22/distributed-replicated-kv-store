package de.tum.i13.client;


import de.tum.i13.shared.Constants;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;
import java.util.concurrent.ThreadLocalRandom;


public class Milestone1Main {
    private static final long INITIAL_DELAY = 200;
    private static final double MULTIPLIER = 1.1;
    private static final long RANDOMIZATION_FACTOR = 3;
    private static final int MAX_RETRIES = 10;
    private static final int CAP = 40;
    public static ClientMetaData clientMetaData =  new ClientMetaData();
    public static void main(String[] args) throws IOException, InterruptedException {
        //EmailTest();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        ActiveConnection activeConnection = null;
        for(;;) {
            System.out.print("EchoClient> ");
            String line = reader.readLine();
            String[] command = line.split(" ", 3);
            switch (command[0]) {
                case "connect": activeConnection = buildconnection(command); break;
                case "send": sendmessage(activeConnection, command, line); break;
                case "put": put_key(activeConnection, command, line); break;
                case "get": get_key(activeConnection, command, line); break;
                case "keyrange": get_keyrange(activeConnection, command, line); break;
                case "delete": delete_key(activeConnection, command, line); break;
                case "disconnect": closeConnection(activeConnection); break;
                case "help": printHelp(); break;
                case "quit": printEchoLine("Application exit!"); return;
                default: printEchoLine("Unknown command");
            }
        }
    }
    public static void EmailTest() throws InterruptedException {
        ActiveConnection activeConnection = buildconnection(new String[]{"connect", "127.0.0.1", "5150"});
        final File folder = new File("C:/TUM/Cloud Databases/MS3/maildir");
        String temp = putFiles(folder, activeConnection);
        total_duration = 0;
        num_records = 0;
        String temp2 = getFiles(folder, activeConnection);
        System.out.println("--------------------------------------------");
        System.out.println(temp);
        System.out.println("--------------------------------------------");
        System.out.println(temp2);
        System.out.println("--------------------------------------------");
        /*String[] t2 = temp.split(":");
        long total_dur = Long.parseLong(t2[0]);
        int num_rec = Integer.parseInt(t2[1]);
        System.out.println("Time taken : " + total_dur/num_rec);*/
    }
    static long total_duration = 0;
    static int num_records = 0;
    public static String putFiles(final File folder, ActiveConnection activeConnection) throws InterruptedException {
        //long total_duration = 0;
        //int num_records = 0;
        if( num_records >= 10){
            return total_duration + ":" + num_records;
            //return total_duration + ":" + num_records;
        }
        for (final File fileEntry : folder.listFiles()) {
            //Thread.sleep(400);
            if( num_records >= 10){
                break;
                //return total_duration + ":" + num_records;
            }
            if (fileEntry.isDirectory()) {
                putFiles(fileEntry, activeConnection);
            } else {
                long startTime = System.nanoTime();
                put_key(activeConnection, new String[]{"put", fileEntry.getName(), fileEntry.toString()}, "put " + fileEntry.getName() + " " + fileEntry.toString());
                long endTime = System.nanoTime();
                total_duration += (endTime - startTime);  //divide by 1000000 to get milliseconds.
                num_records += 1;
                System.out.println("Total Duration : " + total_duration);
                System.out.println("Number of Records : " + num_records);
            }
        }
        return total_duration + ":" + num_records;
        //System.out.println("Avg. Time = " + (total_duration/num_records)/new Double(1000000) + "milliseconds");
    }
    public static String getFiles(final File folder, ActiveConnection activeConnection) throws InterruptedException {
        if( num_records >= 10){
            return total_duration + ":" + num_records;
            //return total_duration + ":" + num_records;
        }
        for (final File fileEntry : folder.listFiles()) {
            //Thread.sleep(400);
            if( num_records >= 10){
                break;
                //return total_duration + ":" + num_records;
            }
            if (fileEntry.isDirectory()) {
                getFiles(fileEntry, activeConnection);
            } else {
                long startTime = System.nanoTime();
                get_key(activeConnection, new String[]{"get", fileEntry.getName()}, "get " + fileEntry.getName());
                long endTime = System.nanoTime();
                total_duration += (endTime - startTime);  //divide by 1000000 to get milliseconds.
                num_records += 1;
                System.out.println("Total Duration : " + total_duration);
                System.out.println("Number of Records : " + num_records);
            }
        }
        return total_duration + ":" + num_records;
        //System.out.println("Avg. Time = " + (total_duration/num_records)/new Double(1000000) + "milliseconds");
    }
    public static String get_keyrange(ActiveConnection activeConnection, String[] command, String line) {
        if (command.length == 1){
            activeConnection.write(line);
            try {
                String response = activeConnection.readline();
                String[] splitResponse = response.split(" ");
                if (splitResponse[0].equalsIgnoreCase("keyrange_success")){
                    clientMetaData = new ClientMetaData();
                    String[] ranges = splitResponse[1].split(";");
                    String[] kvRangeData;
                    for (String element: ranges){
                        kvRangeData = element.split(",");
                        String ipAddress = kvRangeData[2].split(":")[0];
                        String port = kvRangeData[2].split(":")[1];
                        String rangeFrom = kvRangeData[0];
                        String rangeTo = kvRangeData[1];
                        clientMetaData.put(kvRangeData[2], rangeFrom, rangeTo);
                    }
                }
                printEchoLine(response);
                return response.split(" ", 2)[1];
            } catch (IOException e) {
                printEchoLine("Error! Not connected!");
            }
        }
        else {
            printEchoLine("Invalid number of arguments: usage: put <key> <value>");
            return null;
        }
        return null;
    }

    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication protocol.");
        System.out.println("logLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        System.out.println("help - Display this help");
        System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
    }

    private static void printEchoLine(String msg) {
        System.out.println("EchoClient> " + msg);
    }

    private static void closeConnection(ActiveConnection activeConnection) {
        if(activeConnection != null) {
            try {
                activeConnection.close();
            } catch (Exception e) {
                activeConnection = null;
            }
        }
    }

    public static boolean in_range(String hash, String range_min, String range_max){
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

    public static String get_md5_hash(String value) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(value.getBytes());
        byte[] digest = md.digest();
        return DatatypeConverter.printHexBinary(digest);
    }
    private static void put_key(ActiveConnection activeConnection, String[] command, String line){
        activeConnection.write(line); // for sending stuff to server
        try {
            //printEchoLine(activeConnection.readline()); // getting stuff from server
            String response = activeConnection.readline();
            String[] splitResponse = response.split(" ");
            if (splitResponse[0].equalsIgnoreCase("put_success") || splitResponse[0].equalsIgnoreCase("put_update") || splitResponse[0].equalsIgnoreCase("put_error")){
                printEchoLine(response);
            }
            else {
                if (splitResponse[0].equalsIgnoreCase("keyrange_success")){/*
                    String[] datasets = splitResponse[1].split(";");
                    for (String dataset : datasets){
                        System.out.println(dataset);
                        if (in_range(get_md5_hash(command[1]), dataset.split(",")[2].split(":")[0], dataset.split(",")[2].split(":")[1])){
                            activeConnection = buildconnection(new String[]{"connect", dataset.split(",")[2].split(":")[0], dataset.split(",")[2].split(":")[1]});
                            put_key(activeConnection, command, line);
                        }
                    }
                */}
                else if (splitResponse[0].equalsIgnoreCase("server_not_responsible")){
                    String keyrange = get_keyrange(activeConnection, "keyrange".split(" "), "keyrange");
                    String[] datasets = keyrange.split(";");
                    for (String dataset : datasets){
                        if (in_range(get_md5_hash(command[1]), dataset.split(",")[0], dataset.split(",")[1])){
                            activeConnection = buildconnection(new String[]{"connect", dataset.split(",")[2].split(":")[0], dataset.split(",")[2].split(":")[1]});
                            put_key(activeConnection, command, line);
                            break;
                        }
                    }
                } else if (splitResponse[0].equalsIgnoreCase("server_stopped") || splitResponse[0].equalsIgnoreCase("server_write_lock")) {
                    int retries = 0;
                    boolean retry = true;
                    int randomNum = ThreadLocalRandom.current().nextInt(0, (int) Math.min(CAP, Math.pow(2, retries) * INITIAL_DELAY));
                    while (retry && (retries < MAX_RETRIES)){
                        activeConnection.write(line);
                        response = activeConnection.readline();
                        if (response.split(" ")[0].equalsIgnoreCase("put_success") || response.split(" ")[0].equalsIgnoreCase("put_update") || response.split(" ")[0].equalsIgnoreCase("put_error")){
                            retry = false;
                            printEchoLine(response);
                        }
                        else {
                            retries += 1;
                        }
                    }
                }
                else {
                    printEchoLine(response);
                }
            }
        } catch (IOException e) {
            printEchoLine("Error! Not connected!");
        }

    }

    private static void get_key(ActiveConnection activeConnection, String[] command, String line){
        if (command.length == 2){
            activeConnection.write(line);
            try {
                //printEchoLine(activeConnection.readline());
                String response = activeConnection.readline();
                String[] splitResponse = response.split(" ");
                if (splitResponse[0].equalsIgnoreCase("get_success") || splitResponse[0].equalsIgnoreCase("get_update") || splitResponse[0].equalsIgnoreCase("get_error")){
                    printEchoLine(response);
                }
                else {
                    if (splitResponse[0].equalsIgnoreCase("server_not_responsible")){
                        String keyrange = get_keyrange(activeConnection, "keyrange".split(" "), "keyrange");
                        String[] datasets = keyrange.split(";");
                        for (String dataset : datasets){
                            if (in_range(get_md5_hash(command[1]), dataset.split(",")[0], dataset.split(",")[1])){
                                activeConnection = buildconnection(new String[]{"connect", dataset.split(",")[2].split(":")[0], dataset.split(",")[2].split(":")[1]});
                                get_key(activeConnection, command, line);
                                break;
                            }
                        }
                    } else if (splitResponse[0].equalsIgnoreCase("server_stopped") || splitResponse[0].equalsIgnoreCase("server_write_lock")) {
                        int retries = 0;
                        boolean retry = true;
                        int randomNum = ThreadLocalRandom.current().nextInt(0, (int) Math.min(CAP, Math.pow(2, retries) * INITIAL_DELAY));
                        while (retry && (retries < MAX_RETRIES)){
                            activeConnection.write(line);
                            response = activeConnection.readline();
                            if (response.split(" ")[0].equalsIgnoreCase("get_success") || response.split(" ")[0].equalsIgnoreCase("get_update") || response.split(" ")[0].equalsIgnoreCase("get_error")){
                                retry = false;
                                printEchoLine(response);
                            }
                            else {
                                retries += 1;
                            }
                        }
                    }
                    else {
                        printEchoLine(response);
                    }
                }
            } catch (IOException e) {
                printEchoLine("Error! Not connected!");
            }
        }
        else{
            printEchoLine("Invalid number of arguments: usage: get <key>");
        }
    }

    private static void delete_key(ActiveConnection activeConnection, String[] command, String line){
        if (command.length == 2){
            activeConnection.write(line);
            try {
                //printEchoLine(activeConnection.readline());
                String response = activeConnection.readline();
                String[] splitResponse = response.split(" ");
                if (splitResponse[0].equalsIgnoreCase("delete_success") || splitResponse[0].equalsIgnoreCase("delete_error")){
                    printEchoLine(response);
                }
                else {
                    if (splitResponse[0].equalsIgnoreCase("server_not_responsible")){
                        get_keyrange(activeConnection, "keyrange".split(" "), "keyrange");
                    } else if (splitResponse[0].equalsIgnoreCase("server_stopped") || splitResponse[0].equalsIgnoreCase("server_write_lock")) {
                        int retries = 0;
                        boolean retry = true;
                        int randomNum = ThreadLocalRandom.current().nextInt(0, (int) Math.min(CAP, Math.pow(2, retries) * INITIAL_DELAY));
                        while (retry && (retries < MAX_RETRIES)){
                            activeConnection.write(line);
                            response = activeConnection.readline();
                            if (response.split(" ")[0].equalsIgnoreCase("delete_success") || response.split(" ")[0].equalsIgnoreCase("delete_error")){
                                retry = false;
                                printEchoLine(response);
                            }
                            else {
                                retries += 1;
                            }
                        }
                    }
                    else {
                        printEchoLine(response);
                    }
                }
            } catch (IOException e) {
                printEchoLine("Error! Not connected!");
            }
        }
        else{
            printEchoLine("Invalid number of arguments: usage: get <key>");
        }
    }

    private static void sendmessage(ActiveConnection activeConnection, String[] command, String line) {
        if(activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        int firstSpace = line.indexOf(" ");
        if(firstSpace == -1 || firstSpace + 1 >= line.length()) {
            printEchoLine("Error! Nothing to send!");
            return;
        }

        String cmd = line.substring(firstSpace + 1);
        activeConnection.write(cmd);

        try {
            printEchoLine(activeConnection.readline());
        } catch (IOException e) {
            printEchoLine("Error! Not connected!");
        }
    }

    private static ActiveConnection buildconnection(String[] command) {
        if(command.length == 3){
            try {
                EchoConnectionBuilder kvcb = new EchoConnectionBuilder(command[1], Integer.parseInt(command[2]));
                ActiveConnection ac = kvcb.connect();
                String confirmation = ac.readline();
                printEchoLine(confirmation);
                return ac;
            } catch (Exception e) {
                //Todo: separate between could not connect, unknown host and invalid port
                printEchoLine("Could not connect to server");
            }
        }
        return null;
    }
}
