package com.atguigu.prome.util;



import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class DimUtil {


    static Map<String,String> userRedisCache=new HashMap();
    static  {
        userRedisCache.put("001","zhang3");
        userRedisCache.put("002","li4");
    }

    static Map<String,String> userHbaseDB=new HashMap();
    static  {
        userHbaseDB.put("001","zhang3");
        userHbaseDB.put("002","li4");
        userHbaseDB.put("003","linghuchong");
        userHbaseDB.put("004","yanghuo");
    }


    public static Tuple2<String,Boolean> getDimInfo(String key){
        String value=null;
        String valueFromCache = userRedisCache.get(key);
        if(valueFromCache!=null){
            System.out.println("cache hit");
            return new Tuple2<>( valueFromCache,true);  //缓存命中
        }else{  //缓存未命中
            System.out.println("cache miss");
            String valueFromDB = userHbaseDB.get(key);
            return new Tuple2<>( valueFromDB,false);
        }



    }
}
