package day05;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink的重启策略是针对Task (subTask)级别进行重启的
 *
 * Flink的重启策略,可以根据配置的策略,灵活实现程序的容错,减少人为干预
 *
 * Flink的重启策略一般结合状态使用,可以保证数据的一致性
 *
 * 恢复策略,所有的task都需要重启
 *
 * 按照一段时间的错误率执行重启策略
 *
 */
public class demo05_RestartStrategy_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //设置  在一段时间内允许最多的重启次数, 一段时间, 延迟重启时间
        //在30s内,重启次数不超过3次,程序可以重启, 如果超过了30s, 重新对出现的错误计数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30),Time.seconds(2)));
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    //人为制造异常
                    if (word.startsWith("error")) {
                        throw new RuntimeException();
                    }
                    collector.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = maped.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum("f1");

        sumed.print();

        env.execute();

    }
}
