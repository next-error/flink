package day08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * /Flink的侧流输出
 * 本质上,将一个DataStream中的数据打上一个或者多个标签,然后根据需要,取出对应标签的数据
 *
 * 优势:  比Filter的效率高 (filter多次过滤,需要将同样的数据进行拷贝)
 *
 *
 * 将数据根据奇数 偶数 字符串 打上三种标签
 *
 *
 */
public class demo06_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //1
        //2
        //aaa
        //定义标签
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag") { };
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag") { };
        OutputTag<String> strTag = new OutputTag<String>("str-tag") { };

        //底层的process方法才可以打标签
        //数据按照是否打标签,分为主流 和 非主流
        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                try {
                    int num = Integer.parseInt(value);
                    if (num % 2 != 0) {
                        //奇数
                        ctx.output(oddTag, num);  //侧流输出
                    } else {
                        ctx.output(evenTag, num);
                    }
                } catch (NumberFormatException e) {
                    //字符串类型
                    ctx.output(strTag, value);
                }
                //输出没有打标签的数据
                out.collect(value);
            }
        });
        DataStream<Integer> oddStream = mainStream.getSideOutput(oddTag);
        DataStream<String> strStream = mainStream.getSideOutput(strTag);
        oddStream.print("odd: ");
        strStream.print("str ");
        //输出主流数据
        mainStream.print("main ");
        env.execute();
    }
}
