package day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义函数:
 *
 * Udf: 输入一个,输出一个
 */
public class demo06_UDFsql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 9999);
        //province,city
        //辽宁省,沈阳市
        SingleOutputStreamOperator<Tuple2<String, String>> map = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {

                String[] fields = value.split(",");
                String province = fields[0];
                String city = fields[1];
                return Tuple2.of(province, city);
            }
        });

        tEnv.createTemporaryView("v_data",map);

        //注册函数
        tEnv.createTemporaryFunction("My_CONCAT_WS",new My_CONCAT_WS());

        TableResult tableResult = tEnv.executeSql("select My_CONCAT_WS('-', f0, f1) location from v_data");
        tableResult.print();
        env.execute();


    }

    public static class My_CONCAT_WS extends ScalarFunction{
        //方法名必须为eval
        public String eval (String sp ,String f1, String f2){
            return f1 + sp + f2;
        }
    }
}
