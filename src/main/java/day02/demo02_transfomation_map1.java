package day02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class demo02_transfomation_map1 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        DataStreamSource<String> words = env.socketTextStream("linux01",8888);
        //订单id,商品分类
        //o100,1
        //o101,2
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> maped = words.map(new CategoryNameMapFunction());

        //处理后的数据
        //o100,1,家具
        //o101,2,手机



        maped.print();
        env.execute();

    }
    public static class CategoryNameMapFunction extends RichMapFunction<String, Tuple3<String, Integer, String>> {
        Connection conn ;
        String name = "未知";
        PreparedStatement preparedStatement ;
        ResultSet resultSet ;
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://linux01:3306/test?characterEncoding=utf-8", "root", "root");

        }

        @Override
        public Tuple3<String, Integer, String> map(String s) throws Exception {
            String[] splited;
            Integer cid;
            try {
                splited = s.split(",");
                cid = Integer.parseInt(splited[1]);
                 preparedStatement = conn.prepareStatement("select name from tb_aa where oid = ?");
                preparedStatement.setInt(1,cid);
                resultSet = preparedStatement.executeQuery();
                if (resultSet.next())
                   name = resultSet.getString(1);
            } finally {
                if (resultSet != null)
                resultSet.close();
                if (preparedStatement != null)
                preparedStatement.close();
            }


            return Tuple3.of(splited[0],cid,name);
        }

        @Override
        public void close() throws Exception {
            conn.close();
        }
    }

}
