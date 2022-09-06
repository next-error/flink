package day07.day13;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import scala.Int;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义聚合函数 (多行返回一行,一个组返回一行)
 *
 */
public class demo02_UDAF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEev = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //tom,14,male
        //计算平均年龄
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Integer.parseInt(fields[1]), fields[2]);
            }
        });

        tEev.createTemporaryView("v_user",tpStream,$("name"),$("age"),$("gender"));

        //自定义函数


        tEev.createTemporaryFunction( "myavg",MyAvgFunction.class);

        TableResult tableResult = tEev.executeSql("select gender, myavg(age) avg_age from v_user group by gender");
        tableResult.print();

        env.execute();

    }

    public static class MyAvgFunction extends AggregateFunction<Double, Tuple2<Double,Integer>>{

        //返回结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
           return (accumulator.f0 / accumulator.f1);
        }
        //创建一个初始值
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        public void accumulate(Tuple2<Double,Integer> acc, Integer age){
            acc.f0 += age;
            acc.f1 += 1;
        }

    }
}
