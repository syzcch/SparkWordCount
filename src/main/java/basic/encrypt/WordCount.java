package basic.encrypt;

/**
 * Created by rogersong on 21/03/17.
 */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 *./spark-submit --class basic.encrypt.WordCount /Users/rogersong/latiPay/spark-java-wordcount/target/roger-test-forLatipay-1.0-SNAPSHOT.jar /Users/rogersong/latiPay/spark/spark-2.2.0-bin-hadoop2.7/README.md /Users/rogersong/latiPay/spark/spark-2.2.0-bin-hadoop2.7/output2
 */
public final class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        String outputFile = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        counts.saveAsTextFile(outputFile);
        spark.stop();
    }
}
