package org.apache.jmeter.reporters;

import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.jmeter.util.JMeterUtils;

import java.util.Properties;

/**
 * @author: tong.wang
 * @since: 12/1/16 7:19 PM
 * @version: 1.0.0
 */
public class KafkaProducer {

    private static Producer producer = null;

    private KafkaProducer() {

        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "172.17.103.186:9092,172.17.103.187:9092,172.17.103.188:9092");


        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks", "-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    static class Inner {
        static KafkaProducer kafkaProducer = new KafkaProducer();
    }

    public static KafkaProducer getInstance() {
        return Inner.kafkaProducer;
    }

    public void send(String msg) {
        try {

            producer.send(new KeyedMessage(KafkaTopics.YPT_KAFKA_TASK_DATA_TOPIC, msg));
        } catch (Exception e) {
            System.out.println("failed to send kafka msg " + e);
        }

    }

    public void send(String topic, String msg) {
        try {
            producer.send(new KeyedMessage(topic, msg));
        } catch (Exception e) {
            System.out.println("failed to send kafka msg " + e);
        }
    }

    public static class KafkaTopics {
        public static String YPT_KAFKA_TASK_DATA_TOPIC = "YPT_KAFKA_TASK_DATA_TOPIC";
        public static String YPT_KAFKA_JMETER_ERROR_TOPIC = "YPT_KAFKA_JMETER_ERROR_TOPIC";
        public static String YPT_KAFKA_JEMTER_END_TOPIC = "YPT_KAFKA_JEMTER_END_TOPIC";
    }
}