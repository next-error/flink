package day11;

import cn.doitedu.day10.MyKafkaSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 如果保证ExactlyOnce
 *
 * Flink支持多种一致性语义，AtLeastOnce 和 ExactlyOnce
 *
 * AtLeastOnce 至少一次，即数据至少被处理一次，但是有可能不重复处理
 * ExactlyOnce 精准一次，即数据被精准处理一次
 *
 * AtLeastOnce前提条件
 * 从Kafka中读取数据，然后写入到Redis中，开启Checkpointing，可以保证AtLeastOnce（Source可以记录偏移量，Sink支持覆盖）
 *
 * ExactlyOnce前提条件
 * 从Kafka中读取数据，然后吸入到Kafka或MySQL中，开启Checkpointing，可以保证ExactlyOnce（Source可以记录偏移量，Sink支持事务）
 *
 * 当前的例子：
 *   从Kafka中读取数据，然后将数据写入到Kafka中
 *
 * 原理
 * FlinkKafkaProducer继承了TwoPhaseCommitSinkFunction抽象类，该类实现了CheckpointedFunction, CheckpointListener
 * ①调用initializeState初始化一个OperatorState
 * ②当有数据输入，就会调用invoke方法，将数据调用开启事务的Producer的send方法（缓存到客户端）
 * ③当达到了checkpoint的时间，会调用snapshotState方法，进行preCommit，然后将数据保存到OperatorState中
 * ④jobManager集齐了所有subtaks checkpoint成功的ack消息后，会向实现了CheckpointListener的subtask发送rpc消息，让其调用notifyCheckpointComplete方法，然后提交事务
 *
 * 如果提交事务失败，subtask重启，initializeState恢复数据并且重新写入数据并提交事务
 *
 *
 */
public class demo01_ConnKafka_NewAPI {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用新的KafkaAPI
        KafkaSource<String> source = KafkaSource.<String>builder()  //泛型指定的是读取数据的类型
                .setBootstrapServers("linux01:9092,linux02:9092,linux02:9092,")  //kafka地址
                .setTopics("input-topic") //topic,可以是一个或多个
                .setGroupId("my-group") //消费者组ID  在checkPoint时,将偏移量保存在特殊topic中(__consumer_offsets)
//                .setStartingOffsets(OffsetsInitializer.earliest()) //读取偏移量的位置
                .setStartingOffsets(OffsetsInitializer.committedOffsets()) //从状态中读取已经提交的偏移量.如果状态中没有,再去kafka特殊topic中去读
                .setProperty("commit.offsets.on.checkpoint", String.valueOf(false))
                .setValueOnlyDeserializer(new SimpleStringSchema()) //反序列化器
                .build();

        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //开启Checkpoint，默认的CheckpointingMode为EXACTLY_ONCE，如果Source或Sink不支持EXACTLY_ONCE，会降级成AT_LEAST_ONCE
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        //设置flink job的checkpoint对应的StateBackend（状态存储后端）
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(args[0]);
/*
        //Kafka旧版链接
        //设置参数的properties
        Properties properties = new Properties();
        //Kafka的Broker地址
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        //消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false"); //该参数没有生效
        //如果没有记录历史偏移量就从头读
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("wc", new SimpleStringSchema(), properties);


        //当flink checkpoint成功后，不自动提交偏移量到Kafka特殊的topic中(__consumer_offsets)
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);
*/
        SingleOutputStreamOperator<String> res = lines.filter(line -> !line.startsWith("error"));



        //将最终的结果写入到Kafka中







/*        //旧版
        //往Kafka中写入，要创建Kafka的Producer
        //使用的AtLeastOnce
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
               "kafka-out", //topic名称
                new SimpleStringSchema(),
                properties
        );

        String topic = "kafka-out";

        //事务超时时间改为10分钟
        properties.setProperty("transaction.timeout.ms", "600000"); //broker默认值是为15分钟
        //ExactlyOnce
        FlinkKafkaProducer<String> kafkaProducer1 = new FlinkKafkaProducer<>(
                topic,
                new MyKafkaSerializationSchema(topic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        res.addSink(kafkaProducer);
*/
        env.execute();

    }

}
