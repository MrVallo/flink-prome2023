package com.atguigu.prome.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ThreadPoolUtil {
    private static  ThreadPoolExecutor threadPoolExecutor= initPool();
    {
        System.out.println(123);
    }

    private static ThreadPoolExecutor initPool(){

                    System.out.println("~~开辟线程池~~");
                    threadPoolExecutor = new ThreadPoolExecutor(
                        4,4,300, TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                    return threadPoolExecutor;
    }


    public static  ThreadPoolExecutor getInstance(){
        return threadPoolExecutor;
    }


}
