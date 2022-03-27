package com.knn3.rt.scene.ifcondition.sink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author apophis
 * @File KafkaSink
 * @Time 2022/3/26 22:16
 * @Description 工程描述
 */
public class KafkaSink {
    public static FlinkKafkaProducer<SinkModel> sinkWithKey(String brokers, String topic) {
        return new FlinkKafkaProducer<>(topic,
                (element, timestamp)
                        -> new ProducerRecord<>(topic, element.getK().getBytes(StandardCharsets.UTF_8), element.getV().getBytes(StandardCharsets.UTF_8))
                , KafkaSink.buildProperties(brokers), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    public static FlinkKafkaProducer<SinkModel> sinkNoKey(String brokers, String topic) {
        return new FlinkKafkaProducer<>(topic,
                (element, timestamp)
                        -> new ProducerRecord<>(topic, element.getV().getBytes(StandardCharsets.UTF_8))
                , KafkaSink.buildProperties(brokers), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }


    private static Properties buildProperties(String brokers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10240000");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "1000");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5000");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3600000");
        // "all":吞吐量最差,持久性最高    "1":吞吐量适中,持久性适中   "0":吞吐量最高,持久性最差
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        return properties;
    }
}
