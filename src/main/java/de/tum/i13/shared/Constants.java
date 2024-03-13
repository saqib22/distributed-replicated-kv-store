package de.tum.i13.shared;

public class Constants {
	public static final String TELNET_ENCODING = "ISO-8859-1"; // encoding for telnet
	public static final String STORAGE_DIRECTORY_NAME = "target/kv_pairs";
	public static final int KEY_SIZE_IN_BYTES = 20;
	public static final String MAX_RING_RANGE = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; // Maximum 128-bit hex
	public static final String MIN_RING_RANGE = "00000000000000000000000000000000"; // Minimum 128-bit hex
	public static final int WAL_SIZE = 0x8FFFFFF;   //write-ahead-loggin file size ~150 MB
	public static final int ECS_DOWN_TIME = 5; //seconds
}
