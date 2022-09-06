package day06;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MySunByCities extends KeyedProcessFunction<String, Tuple3<String,String,Integer>, Tuple3<String,String,Integer>>{
    private transient MapState<String, Integer> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> citySUmState = new MapStateDescriptor<>("citySUmState", String.class, Integer.class);
         mapState = getRuntimeContext().getMapState(citySUmState);
    }

    @Override
    public void processElement(Tuple3<String, String, Integer> input, Context ctx, Collector<Tuple3<String ,String, Integer>> out) throws Exception {
        String province = input.f0;
        String city = input.f1;
        Integer moneyByPro = mapState.get(city);
        if (moneyByPro == null)
            moneyByPro = 0;
        moneyByPro += input.f2;
        mapState.put(city,moneyByPro);

        out.collect(Tuple3.of(province,city,moneyByPro));


    }
}

