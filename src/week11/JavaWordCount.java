import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;
public final class JavaWordCount {
        public static void main(String [] args) throws Exception {
                if(args.length < 1) {
                        System.out.println("Usage : JavaWordCount <file>");
                        System.exit(1);
                }

                SparkSession spark = SparkSession
                        .builder()
                        .appName("JavaWordCount")
                        .getOrCreate();
                // read file
                JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
                // split text file with " "
                FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
                        public Iterator<String> call(String s) {
                                return Arrays.asList(s.split(" ")).iterator();
                        }
                };
                // mapper
                JavaRDD<String> words = lines.flatMap(fmf);

                PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
                        public Tuple2<String, Integer> call (String s) {
                                return new Tuple2(s, 1);
                        }
                };

                JavaPairRDD<String, Integer> ones = words.mapToPair(pf);
                // reducer
                Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
                        public Integer call(Integer x, Integer y) {
                                return x + y;
                        }
                };

                JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
                // write result
                counts.saveAsTextFile(args[1]);
                spark.stop();
        }

}
