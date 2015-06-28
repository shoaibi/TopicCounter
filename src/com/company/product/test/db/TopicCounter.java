package com.company.product.test.db;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * class TopicCounter
 * Class used to manage counters for topics, including operations such as
 * incrementing, getting counter for a specific topic or reseting, getting counters for all topics.
 * @author shoaibi
 * @version 0.1
 */
abstract public class TopicCounter {
    /**
     * Create the table
     */
    static {
        try {
            TableManager.createTopicCounterTable();
            System.out.println("Table created");
        } catch (IOException e) {
            System.err.println("Failed to create table");
            e.printStackTrace();
        }
    }

    /**
     * Reset counters for all tables
     */
    public static void reset() {
        TableManager.resetCounters();
    }

    /**
     * Increment counter for provided topic name
     * @param topicName String
     */
    public static void increment(String topicName) {
        try {
            long newValue = TableManager.incrementTopicCounter(topicName);
            System.out.println(topicName + "'s counter updated to: " + newValue);
        } catch (IOException ioe) {
            System.err.println("Unable to update counter for: " + topicName);
            ioe.printStackTrace();
        }
    }

    /**
     * Print counters for all topics
     */
    public static void getAll() {
        try {
            Iterator it = TableManager.getAllTopicsCounters().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.println(pair.getKey() + " = " + pair.getValue());
                it.remove(); // avoids a ConcurrentModificationException
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * Print counter for the provided topic name
     * @param topicName String
     */
    public static void getForTopic(String topicName) {
        try {
            long counter = TableManager.getTopicCounter(topicName);
            print(topicName, counter);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (NullPointerException npe) {
            // NullPointerException would happen if the record for that topicName does not exist yet
            // Showing 0 as the counter value would be the logical thing
            print(topicName, Long.valueOf(0));
        }
    }

    /**
     * Simple wrapper to standardize printing of table-counter information
     * @param topicName String
     * @param count Long
     */
    private static void print(String topicName, Long count)
    {
        System.out.println(topicName + " = " + count);
    }
}
