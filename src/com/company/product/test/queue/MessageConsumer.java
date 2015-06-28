package com.company.product.test.queue;

import com.company.product.test.db.TopicCounter;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * class MessageConsumer
 * Class that talks with kafka and consumes messages provided for specified topic by asking
 * @see TopicCounter.increment()
 * @author shoaibi
 * @version 0.1
 */
public class MessageConsumer extends  Thread {
    /**
     * Topic name to bind consumer to
     */
    String topicName;

    /**
     * Consume connector that binds to the kafka zookeeper
     */
    static ConsumerConnector consumerConnector;

    /**
     * Initialize consumerConnector
     */
    static {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", MessageProducer.HOST + ":" + MessageProducer.ZOOKEEPER_PORT);
        properties.put("group.id","test-group");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    /**
     * Class's entry point. Pass topic name to bind consume to.
     * @param argv String[]
     * @throws IllegalArgumentException
     */
    public static void main(String[] argv) throws IllegalArgumentException {
        if (argv.length != 1 || argv[0] == null)
        {
            throw new IllegalArgumentException(String.valueOf(MessageConsumer.class.toString() + "accepts only one parameter e.g. topicName"));
        }
        MessageConsumer mc = new MessageConsumer(argv[0]);
        mc.start();
    }

    /**
     * Set topic name to consume messages of.
     * @param topic String
     */
    MessageConsumer(String topic) {
        topicName = topic;
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()) {
            // ask TopicCounter to increment count for the topic name.
            TopicCounter.increment(topicName);
            //System.out.println(new String(it.next().message()));
            it.next();
        }

    }
}