package com.zzy.spark.learn.java.app;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class HelloWorldApp {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("hellowWorldBoot").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile("/data/test/sparkdata/helloworld").cache();

        lines.map(new Function<String, Object>() {
            @Override
            public Object call(String s) {
                return s;
            }
        });

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value, Integer toValue) {
                return value + toValue;
            }
        });

        wordsCounts.saveAsTextFile("/data/test/sparkdata/hellwordresult");
    }
}
