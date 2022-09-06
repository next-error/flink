package day06;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

    /**
     * KeyState三种类型
     *  ValueState<Integer>  Map<Key, Integer>
     *  MapState<String,Integer>  Map<Key, Map<String,Integer>>
     *  ListState<Integer>  Map<Key, List<Integer>>
     *
     * 现在演示MapState
     *   省份   城市  金额
     *  河北省,邯郸市,4000
     *  河北省,邯郸市,4000
     *  河北省,保定市,2000
     *  山东省,济南市,3000
     *  山东省,青岛市,5000
     *  山东省,青岛市,5000
     *
     *  按照省份KeyBy, 但在同一个分区内将相同城市的金额累加
     *  再算每个省份的总金额
     */


public class demo_01MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //数据预处理
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> streamOperator = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));

            }
        });
        //根据province分组
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);
        //调用写的函数,将相同城市的数据累加起来,得到 省-市-money 的唯一数据
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res1 = keyedStream.process(new MySunByCities());
        res1.print();

        //将每个省份不同城市的金额累加
        //需要先将上面得到的结果放入map集合中,每当重复的城市出现后,总能用最近的数据累加加入到计算结果中,防止数据被重复计算
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream1 = res1.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> res2 = keyedStream1.process(new MySumByProvince());
        res2.print();

        env.execute();


    }


}
