package day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessionFunction 是Flink更加底层的方法,可以访问Flink程序更底层的属性和方法
 *
 *  优点:更灵活
 *  缺点:使用复杂一点
 *
 *  三种功能:
 *  1.对数据一条一条处理
 *  2.对KeyedStream使用KeyedState
 *  3.对KeyState使用定时器 (类似窗口功能)
 *
 *  使用ValueState
 */
public class demo04_ProcessFunction2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //{"name": "tom", "age": 18, "fv": 9999}


        //process.print();
        env.execute();


    }


}
