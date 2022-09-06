package day09;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 时间,用户id,事件类型,商品id,商品分类id
 * 1000,u1001,view,prudoct01,category001
 * 3000,u1002,view,prudoct01,category001
 * 3200,u1002,view,prudoct02,category012
 * 3300,u1002,click,prudoct02,category012
 * 4000,u1002,click,prudoct01,category001
 * 5000,u1003,view,prudoct02,category001
 * 6000,u1004,view,prudoct02,category001
 * 7000,u1005,view,prudoct02,category001
 * 8000,u1005,click,prudoct02,category001
 *
 *
 * 统计10秒钟内的各种事件、各种商品分类下的热门商品TopN，每2秒出一次结果
 *
 *
 * 划分什么样的窗口（窗口类型，时间类型），数据如何聚合、排序
 *
 * 排序要将增量聚合的结果攒起来,再全量聚合
 */
public class demo01_HotGoodsTOPN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //提取EventTime生成WaterMark
        SingleOutputStreamOperator<String> timestampsAndWatermarks = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, time) -> Long.parseLong(line.split(",")[0])));

        SingleOutputStreamOperator<Tuple3<String, String, String>> streamOperator = timestampsAndWatermarks.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                String event = fields[2];
                String productID = fields[3];
                String categoryID = fields[4];
                return Tuple3.of(event, productID, categoryID);
            }
        });
        //根据事件类型 商品id 品类id keyBy
        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = streamOperator.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Tuple3<String, String, String> value) throws Exception {

                return value;
            }
        });
        KeyedStream<String, Tuple3<String, String, String>> keyedStream1 = timestampsAndWatermarks.keyBy(new KeySelector<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(String s) throws Exception {
                String[] fields = s.split(",");
                String event = fields[2];
                String productID = fields[3];
                String categoryID = fields[4];
                return Tuple3.of(event, productID, categoryID);
            }
        });
        WindowedStream<String, Tuple3<String, String, String>, TimeWindow> windowedStream1 = keyedStream1.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        //根据业务需求,需要开窗
        WindowedStream<Tuple3<String, String, String>, Tuple3<String, String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));

        //开窗后,窗口内对数据聚合操作 可以用reduce 也可以用aggregate
        //经过event productID categoryID 分组后,没来一条数据 做一次 +1 最后返回次数
        //业务要求每过两秒计算一次,即根据EventTime每隔两秒触发一次窗口,每次窗口触发输出的次数要标注哪次窗口触发的
        //标注窗口可以用窗口的开始和结束时间标记,实现窗口函数,拿到上下文即可
        SingleOutputStreamOperator<MyHotGoodsItem> aggregate1 = windowedStream.aggregate(new MyHotGoodsAggregateFunction(), new MyHotGoodsWindowFunction());

        //聚合完毕后需要进行排序
        //按照event 和 categoryID 分区,然后将分区内的不同的ProductID 排序
        KeyedStream<MyHotGoodsItem, Tuple2<String, String>> keyedStream2 = aggregate1.keyBy(new KeySelector<MyHotGoodsItem, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MyHotGoodsItem myHotGoodsItem) throws Exception {
                return Tuple2.of(myHotGoodsItem.categoryId, myHotGoodsItem.eventId);
            }
        });

        //排序,需要将窗口的结果存入状态,待下一个窗口触发,该窗口数据肯定不会再更新
        SingleOutputStreamOperator<MyHotGoodsItem> res = keyedStream2.process(new MyHotGoodsOrderFunction());
        res.print();
        env.execute();


    }
}


