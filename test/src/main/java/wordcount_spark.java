
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class wordcount_spark {

    public static void main(String[] args) {

// 1.创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setAppName("wordCountLocal")
                .setMaster("local");

        // 2.创建JavaSparkContext
        // SparkContext代表着程序入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

// 3.读取本地文件
        JavaRDD<String> lines = sc.textFile("C:/Users/lucky/Desktop/spark.txt");

        // 4.每行以空格切割
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });

        // 5.转换为 <word,1>格式
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
        });

        // 6.统计相同Word的出现频率
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 7.执行action，将结果打印出来
        wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1()+" "+t._2());
            }
        });

        // 8.主动关闭SparkContext
        sc.stop();
    }

    }

