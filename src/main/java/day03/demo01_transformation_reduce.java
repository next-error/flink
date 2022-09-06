package day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.用户订单数据
 * u001,河北省,3000
 * u002,山东省,2000
 * u003,河北省,1000
 * u004,山东省,5000
 * u005,浙江省,2000
 *
 * 要求，
 * 1.统计各个省份的总成交金额
 * 2.统计每个省份的成交的订单数量
 * 3.统计交的订所有订单数量
 * 4.统计交的订所有成交金额
 */
public class demo01_transformation_reduce {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        DataStreamSource<String> lines = env.socketTextStream("linux01",8888);



        SingleOutputStreamOperator<Tuple3<String, String, Integer>> maped = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                String[] splited = s.split(",");
                int money = Integer.parseInt(splited[2]);
                return Tuple3.of(splited[0], splited[1], money);
            }
        });
        KeyedStream<Tuple3<String, String, Integer>, String> keyed = maped.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> stringStringIntegerTuple3) throws Exception {
                return stringStringIntegerTuple3.f1;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> provinceMoney_sum = keyed.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> t0, Tuple3<String, String, Integer> t1) throws Exception {
                int sum = t0.f2 + t1.f2;
                return Tuple3.of(t0.f0, t0.f1, sum);
            }
        });
        provinceMoney_sum.print();

        //订单数量
        final int[] conut = {1};
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> countWithProvince = keyed.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> stringStringIntegerTuple3, Tuple3<String, String, Integer> t1) throws Exception {
                conut[0]++;
                return Tuple3.of(t1.f0, t1.f1, conut[0]);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> countPro = countWithProvince.map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Integer> t0) throws Exception {
                return Tuple2.of(t0.f1, t0.f2);
            }
        });
        countPro.print();

        //所有订单数量

        env.execute();


    }
}
