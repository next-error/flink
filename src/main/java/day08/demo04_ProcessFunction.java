package day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.tools.nsc.doc.base.comment.Body;

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
 */
public class demo04_ProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //{"name": "tom", "age": 18, "fv": 9999}

        //1.将JSON字符串解析, 过滤掉问题数据
        SingleOutputStreamOperator<Boy> process = lines.process(new ProcessFunction<String, Boy>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Boy>.Context ctx, Collector<Boy> out) throws Exception {

                try {
                    Boy boy = JSON.parseObject(value, Boy.class);
                    //将数据输出
                    out.collect(boy);
                } catch (Exception e) {
                    //将问题数据保存起来
                }
            }
        });
        process.print();
        env.execute();


    }
    public static class Boy {
        public String name;
        public Integer age;
        public Double fv;

        public Boy() {
        }

        public Boy(String name, Integer age, Double fv) {
            this.name = name;
            this.age = age;
            this.fv = fv;
        }

        @Override
        public String toString() {
            return "Boy{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", fv=" + fv +
                    '}';
        }
    }
}
