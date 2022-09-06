package day10;

import cn.doitedu.day06.C08_KafkaToRedisWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * flink 支持多种语义, AtLeastOnce 和 ExactlyOnce
 *
 *  AtLeastOnce: 至少一次,即数据至少被处理一次,但是有可能被重复处理
 *  ExactlyOnce: 进准一次性, 即数据被处理一次
 *
 *  从Kafka中读取数据,然后写入Redis中, 开启CheckPoint, 可以保证ExactlyOnce (Source 可以记录便宜量,Sink支持覆盖
 *
 *  ExactlyOnce前提条件:
 *      从Kafka中读取数据, 然后写入Kafka或MySQL中 开始CHeckPointing, 可以保证数据的ExactlyOnce Source支持记录偏移量.sink 支持事务
 *
 *  从Kafka中读取数据, 然后将数据写入Kafka中
 *
 */
public class demo03_ExactlyOnce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint,默认为EXACTLY_ONCE
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage(args[0]);
        //设置参数的properties
        Properties properties = new Properties();
        //Kafka的Broker地址
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        //指定消费者组ID
        properties.setProperty("group.id", "test798");
        //消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false");
        //如果没有记录历史偏移量就从头读
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("wc", new SimpleStringSchema(), properties);
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false); //
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<String> res = lines.filter(line -> line.startsWith("error"));
        SingleOutputStreamOperator<Tuple2<String, Integer>> res1 = res.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        //即将一行多个单词进行切分，又将单词和1组合
                        for (String word : line.split(" ")) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(t -> t.f0)
                .sum(1);

      //往Kafka中写入,需要创建kafka的producer --老版本
/*        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "kafka-out",//Topic名称
                new SimpleStringSchema(), //序列化方式
                properties);
        res1.addSink(kafkaProducer);*/

        //新版本



        env.execute();

    }

    //将Flink产生的数据，写入到Redis的映射（就是将数据以何种方式，哪个作为key，哪个作为value）
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        //获取Redis命令的类型(写入的方式)，已经大KEY的名称
        @Override
        public RedisCommandDescription getCommandDescription() {
            //Map(WORD_COUNT, ((spark,1), (hive,5)))
            return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
        }

        //将数据中的哪个字段取出来作为里面的小key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        //将数据中的哪个字段取出来作为里面的小value
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

}
