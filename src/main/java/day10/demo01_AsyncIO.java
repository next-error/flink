package day10;

import cn.doitedu.day10.C01_AsyncIODemo1;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 异步IO的使用场景: 做查询使用
 *      访问大量的数据,这些数据无法直接拿到 (请求数据库,或者是外部接口) 并且希望关联的数据更快,提高程序的吞吐量
 * 异步IO底层:
 *      就是使用了多线程的方式,在一段时间内,使用多线程(线程池) 在同一个subTask中发送更多的请求(异步的)
 *      代价是消耗更多CPU资源
 *
 * 该例子使用异步IO ,请求高德地图逆地理位置API,获取相关信息
 *
 */
public class demo01_AsyncIO {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<OrderBean> beanStream = lines.process(new ProcessFunction<String, OrderBean>() {
            @Override
            public void processElement(String value, ProcessFunction<String, OrderBean>.Context ctx, Collector<OrderBean> out) throws Exception {
                try {
                    OrderBean orderBean = JSON.parseObject(value, OrderBean.class);
                    out.collect(orderBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        SingleOutputStreamOperator<OrderBean> res = AsyncDataStream.unorderedWait(beanStream, new HttpAsyncFunction(), 3, TimeUnit.SECONDS);
        res.print();
        env.execute();
    }



    public static class HttpAsyncFunction extends RichAsyncFunction<OrderBean,OrderBean>{
        private String key;
        private CloseableHttpAsyncClient httpAsyncClient;
        //
        @Override
        public void open(Configuration parameters) throws Exception {
           //初始化可以多线程发送异步请求的客户端
            //创建异步查询的HTTPClient
            //创建一个异步的HTTPClient连接池
            //初始化异步的HTTPClient
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(3000)
                    .setConnectTimeout(3000)
                    .build();
            httpAsyncClient = HttpAsyncClients.custom()
                    .setMaxConnTotal(20)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
            //开启异步查询的线程池
            httpAsyncClient.start();

        }
        //asyncInvoke方法,来一条数据调用一次
        @Override
        public void asyncInvoke(OrderBean orderBean, ResultFuture<OrderBean> resultFuture) throws Exception {
            //在该方法中可以使用多线程查询,方法不必等待方法的返回,就可以对下一条进行异步查查

            try {
                double longitude = orderBean.longitude;
                double latitude = orderBean.latitude;
                //使用Get方式进行查询
                HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?&location=" + longitude + "," + latitude + "&key=" + key);
                //查询结果放入返回Future
                Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);
                //CompletableFuture实现了Future接口,是对Future的扩展和增强
                //CompletableFuture的静态方法supplyAsync,创建一个异步操作
                //supplyAsync需要传入Supplier,重写get方法
                CompletableFuture.supplyAsync(new Supplier<OrderBean>() {
                    @Override
                    public OrderBean get() {
                        String province = null;
                        String city = null;
                        try {
                            //从future中提取数据
                            HttpResponse response = future.get();
                            //判断是否查询到了数据(查询相应的状态)
                            if (response.getStatusLine().getStatusCode() == 200) {
                                //若查到了数据,获取请求到的数据,JSON格式,转为String
                                String result = EntityUtils.toString(response.getEntity());
                                System.out.println(result);

                                //需要转为JSON对象,然后解析信息
                                JSONObject jsonObject = JSON.parseObject(result);
                                //获取位置信息
                                JSONObject regeocode = jsonObject.getJSONObject("geocodes");
                                //数据判空处理,获得regeocode中的省和市字段信息
                                if (regeocode != null && !regeocode.isEmpty()) {
                                    JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                                     province = addressComponent.getString("province");
                                     city = addressComponent.getString("city");
                                }
                            }
                            orderBean.province = province;
                            orderBean.city = city;
                            return orderBean;

                        } catch (Exception e) {
                            return null;
                        }
                    }
                    //thenAccept方法???????
                }).thenAccept((OrderBean result) ->
                {
                    resultFuture.complete(Collections.singleton(result));
                });

            } catch (Exception e) {
                resultFuture.complete(Collections.singleton(null));
            }


        }
    }

    public static class OrderBean{
        public String oid;

        public String cid;

        public Double money;

        public Double longitude;

        public Double latitude;

        public String province;

        public String city;

        public OrderBean() {
        }

        public OrderBean(String oid, String cid, Double money, Double longitude, Double latitude) {
            this.oid = oid;
            this.cid = cid;
            this.money = money;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public static C01_AsyncIODemo1.OrderBean of(String oid, String cid, Double money, Double longitude, Double latitude) {
            return new C01_AsyncIODemo1.OrderBean(oid, cid, money, longitude, latitude);
        }

        @Override
        public String toString() {
            return "OrderBean{" + "oid='" + oid + '\'' + ", cid='" + cid + '\'' + ", money=" + money + ", longitude=" + longitude + ", latitude=" + latitude + ", province='" + province + '\'' + ", city='" + city + '\'' + '}';
        }
    }
}
