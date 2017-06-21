package cn.hadoop;

import org.apache.hadoop.io.Text;

/**
 * Created by wangzhilong on 2017/6/21.
 */

public class AppTest {

    public static void textTest(){
        Text str = new Text();
        str.set("www");
        System.out.println(str.toString());
        str.set("wzl");
        System.out.println( str.toString());
    }

    public static void main(String[] args) {
        textTest();
    }
}
