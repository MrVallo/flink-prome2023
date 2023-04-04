package com.atguigu.prome.app;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Demo01App {

    public static void main(String[] args) throws Exception {

        //0 调试取本地配置 ，打包部署前要去掉
        //Configuration configuration=new Configuration(); //此行打包部署专用
       String resPath = Thread.currentThread().getContextClassLoader().getResource("").getPath(); //本地调试专用
      Configuration configuration = GlobalConfiguration.loadConfiguration(resPath);            //本地调试专用

        //1. 读取初始化环境
        configuration.setString("metrics.reporter.promgateway.jobName","demo01App");  //每个程序不一样 独立配置在程序中写死

        // configuration.setString(RestOptions.BIND_PORT, "19999"); //idea 开启flinkUI用的

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 2. 指定nc的host和port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 3. 接受socket数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        dataStreamSource.print();

        env.execute("demo01App");

    }
}
