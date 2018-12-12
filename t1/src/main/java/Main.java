import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop" );
        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local"); //sets Master URL
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Problem 1
        JavaRDD<String> textFile = sc.textFile("src/main/resources/word_count.text");
        JavaPairRDD<String, Integer> counts = textFile
                .map(line -> line.replaceAll("[();&.,\\[\\]]", "").toLowerCase())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("/tmp/WordCount");

        //Problem 2
        JavaRDD<String> textFile2 = sc.textFile("src/main/resources/prime_nums.text");
        Integer total = textFile2
                .flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                .filter(line -> !line.equals(""))
                .map(Integer::valueOf)
                .reduce((a, b) -> a + b);
        System.out.println(total);

        //Problem 3
        JavaRDD<String> textFile3 = sc.textFile("src/main/resources/airports.text")
                .filter(line -> line.contains("United States"))
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator());
        textFile3.foreach(p -> System.out.println("\"" + p.split("\"")[1] + "\", " + p.split("\"")[3] + "\""));
    }
}
