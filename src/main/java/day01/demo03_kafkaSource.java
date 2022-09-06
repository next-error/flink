package day01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class demo03_kafkaSource {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","linux01:9092,linux02:9092,linux03:linux03:9092");
        properties.setProperty("group.id","g001");
        properties.setProperty("auto.offect.reset","earliest");properties.setProperty("acks", "all");
        FlinkKafkaConsumer<String> flinkafkaConsunmer = new FlinkKafkaConsumer<>("test2", new SimpleStringSchema(), properties);

        DataStreamSource<String> lines = env.addSource(flinkafkaConsunmer);
        lines.print();
        env.execute();


    }
}
