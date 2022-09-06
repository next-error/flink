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
 * 开启checkPoint: 定期将state保存起来, 并且subTask重启后,可以恢复到以前的状态
 *
 * 如果开启checkPoint,默认重启策略是无限重启
 *
 * Flink程序一般都会开启checkPoint,就使用了该策略
 *
 * 若不想使用默认策略,可以自己设置相应的策略
 */
public class demo05_RestartStrategy_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //设置  在一段时间内允许最多的重启次数, 一段时间, 延迟重启时间
        //默认无限重启Integer.MaxValue次
        env.enableCheckpointing(5000); //5秒将状态快照
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
