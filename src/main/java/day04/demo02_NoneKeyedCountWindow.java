package day04;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 *
 * 窗口聚合算子: 在窗口内进行增量聚合,没有达到窗口的触发条件,窗口不会触发,但会将数据进行增量聚合,窗口触发后会输出结果
 *
 * 若调用apply方法,会将数据攒起来(windowState),窗口触发后,再执行对应的逻辑
 *
 * 没有KedBy的window,即NoneWindow,Window和WindowOperator所在的DateStream,并行度为1
 */
public class demo02_NoneKeyedCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //,没有keyBy直接划分窗口
        AllWindowedStream<Integer, GlobalWindow> windowStream = nums.countWindowAll(5);
        //划分窗口后对窗口进行操作,sum, reduce,apply(对window进行处理的算子)
        //SingleOutputStreamOperator<Integer> sumed = windowStream.sum(0);
        SingleOutputStreamOperator<Integer> resStream = windowStream.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
            /**
             *
             * @param window:窗口类型
             * @param input:输入的数据,缓存在WindowState中,窗口触发后,才会调用apply
             * @param out:
             * @throws Exception
             */
            @Override
            public void apply(GlobalWindow window, Iterable<Integer> input, Collector<Integer> out) throws Exception {
                int sum = 0;
                for (Integer i : input) {
                    sum += i;
                }
                out.collect(sum);
            }
        });
        resStream.print();
        //sumed.print();
        env.execute();



    }
}
