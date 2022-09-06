package day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 从kafka读数据,写入Redis
 * 若程序出现异常,保证计算结果正确(AtLeastOnce)
 * 实现容错:
 *  状态 (偏移量, 累加的单词 和次数)
 *  source记录偏移量 ,sink实现结果覆盖 实现了AtLeastOnce
 *  checkPoint
 *
 *
 *  HashMapStateBackend 特点:
 *      可以存储 大量state的 长窗口的 单个K-V比较大的状态数据
 *      支持高可用,可以保存到外部文件系统
 *   在代码中单独配置StateBackend.也可以在flink配置文件 flink-conf.yaml中配置
 */
public class demo01_KafkaToRedisWordCount_HashMapStateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        //设置flink job的checkpoint对应的StateBackend（状态存储后端）
        //env.setStateBackend(new FsStateBackend(args[0]));

        //使用HashMapStateBackend 相比于
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(args[0]);

        //将job在web页面cancel后，不删除最近的checkpoint数据
        //CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION 默认的，即job cancel后，外部的checkpoint数据就删除了
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit","false"); //消费者不自动提交偏移量 但是 该参数没有生效 下面需要配置flinkafkaConsunmer.setCommitOffsetsOnCheckpoints(false);
        properties.setProperty("bootstrap.servers","linux01:9092,linux02:9092,linux03:linux03:9092");
        properties.setProperty("group.id","g001");
        properties.setProperty("auto.offect.reset","earliest");properties.setProperty("acks", "all");
        FlinkKafkaConsumer<String> flinkafkaConsunmer = new FlinkKafkaConsumer<>("kafkaToredis", new SimpleStringSchema(), properties);

        //当flink checkpoint成功后，不自动提交偏移量到Kafka特殊的topic中(__consumer_offsets)
        flinkafkaConsunmer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> lines = env.addSource(flinkafkaConsunmer);
        //lines.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(",");
                for (String word : words)
                    collector.collect(Tuple2.of(word, 1));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum("f1");
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("linux01")
                .setPort(6379)
                .setDatabase(8)
                .build();
         sumed.addSink(new RedisSink<>(conf, new RedisWordCountMapper()));
        //sumed.print();

       // sumed.addSink()
        env.execute();
    }
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
