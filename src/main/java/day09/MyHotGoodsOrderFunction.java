package day09;

import cn.doitedu.day09.C01_HotGoodsTopN;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class MyHotGoodsOrderFunction extends KeyedProcessFunction<Tuple2<String,String>,MyHotGoodsItem,MyHotGoodsItem> {
    private transient ListState<MyHotGoodsItem> listState;
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化状态
        ListStateDescriptor<MyHotGoodsItem> listStateDescriptor = new ListStateDescriptor<>("list-state", MyHotGoodsItem.class);
         listState = getIterationRuntimeContext().getListState(listStateDescriptor);
    }


    @Override
    public void processElement(MyHotGoodsItem value, KeyedProcessFunction<Tuple2<String, String>, MyHotGoodsItem, MyHotGoodsItem>.Context ctx, Collector<MyHotGoodsItem> out) throws Exception {
        listState.add(value);
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }
//触发时机: 下一个窗口触发后
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, MyHotGoodsItem, MyHotGoodsItem>.OnTimerContext ctx, Collector<MyHotGoodsItem> out) throws Exception {
        ArrayList<MyHotGoodsItem> lst = (ArrayList<MyHotGoodsItem>) listState.get();
        lst.sort(new Comparator<MyHotGoodsItem>() {
            @Override
            public int compare(MyHotGoodsItem o1, MyHotGoodsItem o2) {
                return Long.compare(o2.count,o1.count);
            }
        });

        for (int i = 0; i < Math.min(3, lst.size()); i++) {
            MyHotGoodsItem itemEventCount = lst.get(i);
            itemEventCount.order = i + 1;
            out.collect(itemEventCount);
        }

        listState.clear();
    }

}
