package com.haoran;

import com.haoran.utils.ConnectUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // wordcount实例
        JavaSparkContext sc = new ConnectUtil().getJavaSparkContext();

        JavaRDD<String> rdd1 = sc.textFile("src/main/resources/file/hello.txt");  // 读取数据
        // JavaRDD<String> rdd1 = sc.textFile("/hello.txt");  // 读取数据
        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator());  // 切分单词
        JavaPairRDD<String, Integer> pairRDD = rdd2.mapToPair(s -> new Tuple2<>(s, 1)); // 转换格式为(单词,1)
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey((x1, x2) -> x1+x2);  // 聚合相同的key
        System.out.println(result.collect().toString());
        // result.saveAsTextFile("src/main/resources/out1");
        // result.saveAsTextFile("/out1");  // 集群运行

    }
}
