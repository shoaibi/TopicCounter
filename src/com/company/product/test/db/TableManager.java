package com.company.product.test.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;

/**
 * class TableManager Use as backend for @see TopicCounter
 *
 * @author shoaibi
 * @version 0.1
 */
abstract public class TableManager {

    /**
     * Used to store hbase zookeeper configuration
     */
    private static Configuration conf = null;

    /**
     * Table name that is used for storing counters
     */
    private static final String tableName = "topicCounters";

    /**
     * Families associated with tableName
     */
    private static final String[] families = { "counter" };

    /**
     * Initialization
     * Setup Hbase configuration with zookeeper credentials and ensure Hbase is up
     */
    static {
        conf = HBaseConfiguration.create();
        // yay!!!! constants all the way
        conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.20.101");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2020");
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a table
     * @param tableName String
     * @param families String[]
     * @throws IOException
     */
    protected static void createTable(String tableName, String[] families) throws IOException {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            // do we even need to create the table?
            if (admin.tableExists(tableName)) {
                System.out.println(tableName + " already exists!");
            } else {
                // get the descriptor and attach families
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (int i = 0; i < families.length; i++) {
                    tableDesc.addFamily(new HColumnDescriptor(families[i]));
                }
                // time to do the real job
                admin.createTable(tableDesc);
                System.out.println("Created table: " + tableName);
            }
        } catch (ZooKeeperConnectionException | MasterNotRunningException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a table
     * @param tableName
     * @throws IOException
     */
    protected static void deleteTable(String tableName) throws IOException {
        try {
            // easy peasy, rice and cheesy
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Deleted table: " + tableName);
        } catch (ZooKeeperConnectionException | MasterNotRunningException e) {
            e.printStackTrace();
        }
    }

    /**
     * Provide a qualifier, increment its value
     * @param tableName String
     * @param rowKey String
     * @param family String
     * @param qualifier String
     * @return long
     * @throws IOException
     */
    protected static long incrementColumnValue(String tableName, String rowKey, String family, String qualifier)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        // no need to check if the record even exists or not. increment would set it to 1 if the
        // record is missing.
        return table.incrementColumnValue(rowKey.getBytes(), family.getBytes(), qualifier.getBytes(), 1);
    }

    /**
     * Get row represented by rowKey
     * @param tableName String
     * @param rowKey String
     * @return Map<String, Long>
     * @throws IOException
     */
    protected static Map<String, Long> getOneRecord(String tableName, String rowKey) throws IOException {
        Map<String, Long> keyValueMap = new HashMap<>();
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result r = table.get(get);
        keyValueMap.putAll(getKeyValueMapFromResult(r));
        return keyValueMap;
    }

    /**
     * Scan (or list) a table
     * @param tableName String
     * @return  Map<String, Long>
     */
    protected static Map<String, Long> getAllRecords(String tableName) {
        Map<String, Long> keyValueMap = new HashMap<>();
        try {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                keyValueMap.putAll(getKeyValueMapFromResult(r));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyValueMap;
    }

    /**
     * Provided a Result object return a Map containing all KV pairs inside that Result
     * @param r Result
     * @return  Map<String, Long>
     */
    protected static Map<String, Long> getKeyValueMapFromResult(Result r) {
        Map<String, Long> keyValueMap = new HashMap<>();
        for (KeyValue kv : r.raw()) {
            SimpleEntry<String, Long> kvE = getKeyValueEntryFromResult(kv);
            keyValueMap.put(kvE.getKey(), kvE.getValue());
        }
        return keyValueMap;
    }

    /**
     * Provided a KeyValue object compute the SimpleEntry with object's key and value's correct representation
     * @param kv
     * @return SimpleEntry<String, Long>
     */
    protected static SimpleEntry<String, Long> getKeyValueEntryFromResult(KeyValue kv) {
        return new SimpleEntry<>(new String(kv.getRow()), Bytes.toLong(kv.getValue()));
    }

    /**
     * Create the counter table if if it does not exist.
     * Just a package-wide-accessible wrapper around the @see createTable()
     * @throws IOException
     */
    static void createTopicCounterTable() throws IOException {
        createTable(tableName, families);
    }

    /**
     * Provided a topic name increment its counter and return new counter value
     * @param topicName String
     * @return long
     * @throws IOException
     */
    static long incrementTopicCounter(String topicName) throws IOException {
        return incrementColumnValue(tableName, topicName, families[0], "");
    }

    /**
     * Provided a topic name get its counter. Throw NullPointerException if the record does not exist
     * @param topicName String
     * @return long
     * @throws IOException
     * @throws NullPointerException
     */
    static long getTopicCounter(String topicName) throws IOException, NullPointerException {
        return getOneRecord(tableName, topicName).get(topicName);
    }

    /**
     * Get counters for all topics. Just a fancy package-wide-accessible wrapper around @see getAllRecords
     * @return Map<String, Long>
     * @throws IOException
     */
    static Map<String, Long> getAllTopicsCounters() throws IOException {
        return getAllRecords(tableName);
    }

    /**
     * Clean the slate
     */
    static void resetCounters() {
        try {
            deleteTable(tableName);
            TableManager.createTopicCounterTable();
            System.out.println("Counters reset");
        } catch (Exception e) {
            System.err.println("Failed to reset counters");
            e.printStackTrace();
        }
    }
}