package day04;

import Utils.Flink_Local_Connection;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * flink的窗口分为两种,CountWindow 和 TimeWindow
 * CountWindow:按照条数
 *
 *
 * window按是否进行的keyby 分为: KeyedWindow 和 NoneKeyedWindow
 *
 * 没有kyeBy:底层调用windowAll
 *
 */
public class demo01_NoneKeyedCountWindow {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //,没有keyBy直接划分窗口:countWindowAll
        AllWindowedStream<Integer, GlobalWindow> windowStream = nums.countWindowAll(5);
        //划分窗口后对窗口进行操作,sum, reduce,apply(对window进行处理的算子)
        SingleOutputStreamOperator<Integer> sumed = windowStream.sum(0);
        sumed.print();
        env.execute();



    }
}
