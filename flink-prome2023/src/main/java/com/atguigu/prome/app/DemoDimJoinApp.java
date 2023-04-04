package com.atguigu.prome.app;

import com.atguigu.prome.func.DimAsyncFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class DemoDimJoinApp {

    public static void main(String[] args) throws Exception {

        //0 调试取本地配置 ，打包部署前要去掉
         Configuration configuration=new Configuration(); //此行打包部署专用
        //  String resPath = Thread.currentThread().getContextClassLoader().getResource("").getPath(); //本地调试专用
        //   Configuration configuration = GlobalConfiguration.loadConfiguration(resPath);            //本地调试专用

       //1. 读取初始化环境
        configuration.setString("metrics.reporter.promgateway.jobName","prome_dim_join_app");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 2. 指定nc的host和port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 3. 接受socket数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        // 4. 异步关联维度数据
        SingleOutputStreamOperator<String> dataWithDimStream = AsyncDataStream.unorderedWait(dataStreamSource, new DimAsyncFunction(), 10, TimeUnit.SECONDS, 100).name("async_user_join") ;


        dataWithDimStream.print();

        env.execute("dim_join");  //app_name

    }
}
