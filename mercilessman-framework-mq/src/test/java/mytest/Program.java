package mytest;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Arrays;


/**
 * @author tanxiaokang（AZ6342）
 * @description
 * @date 2020/3/25 11:01
 **/


public class Program {
    public static void main(String[] args) throws InterruptedException {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(Config.class);
        Producer producer = applicationContext.getBean(Producer.class);
        for (int i = 0; i < 1000; i++) {
            producer.sendMsg("txk-test"+String.valueOf(i));
            Thread.sleep(500);
        }




    }
}
