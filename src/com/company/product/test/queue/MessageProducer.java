package com.company.product.test.queue;

import kafka.admin.AdminUtils;
import kafka.common.InvalidTopicException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * class Message Producer
 * Class responsible for publishing messages to Kafka Queue for provided topic
 * @author shoaibi
 * @version 0.1
 */
abstract public class MessageProducer {

    /**
     * Hostname used to connect to broker and zookeeper
     * advertised.host.name under zookeeper needs to be set to this, else it could fail.
     */
    final static String HOST = "192.168.20.101";

    /**
     * Kafka broke port for publishing messages
     */
    final static int BROKER_PORT = 9092;

    /**
     * Zookeeper port used to manage topics
     */
    final static short ZOOKEEPER_PORT = 2181;

    /**
     * Kafka Producer instance used to produce messages
     */
    static Producer<String,String> producer;

    /**
     * Setup the Kafka Producer instance
     */
    static {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", HOST + ":" + BROKER_PORT);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        System.out.println("Broker Connect: " + properties.get("metadata.broker.list"));
        producer = new Producer<>(producerConfig);
    }

    /**
     * Produce a message with provided topic name and message.
     * Create a topic if it does not exist
     * @param topicName String
     * @param messageContent String
     */
    public static void sendMessage(String topicName, String messageContent)
    {
        createTopicIfMissing(topicName);
        System.out.println("Sending: " + topicName + "://" + messageContent);
        KeyedMessage<String, String> message = new KeyedMessage<>(topicName, messageContent);
        producer.send(message);
        System.out.println("Message Sent");
        producer.close();
    }

    /**
     * Provided topic name, check if it exists or not. Create if it is inexistent.
     * Throw exception if the topic name is not valid.
     * @param topicName String
     * @throws InvalidTopicException
     */
    private static void createTopicIfMissing(String topicName) throws InvalidTopicException
    {
        final short numberOfPartitions = 1;
        final short replicationFactor = 1;
        final int sessionTimeout = 100000;
        final int connectTimeout = 100000;
        Properties topicConfig = new Properties();
        System.out.println("ZooKeeper Connect: " + HOST + ":" + ZOOKEEPER_PORT);
        ZkClient zkClient = new ZkClient(HOST + ":" + ZOOKEEPER_PORT, sessionTimeout, connectTimeout, ZKStringSerializer$.MODULE$);
        if (!AdminUtils.topicExists(zkClient, topicName))
        {
            AdminUtils.createTopic(zkClient, topicName, numberOfPartitions, replicationFactor, topicConfig);
            System.out.println(topicName + " topic created.");
        }
        else
        {
            System.out.println(topicName + " topic exists.");
        }
    }
}
