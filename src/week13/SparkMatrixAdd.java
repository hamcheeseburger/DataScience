import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;
public class SparkMatrixAdd {
        public static void main(String [] args) {
                SparkSession spark = SparkSession.builder().appName("matrixadd").getOrCreate();

                JavaRDD<String> a_rdd = spark.read().textFile(args[0]).javaRDD();

                JavaPairRDD<String, Integer> a_rdd2 = a_rdd.mapToPair(new PairFunction<String, String, Integer> () {
                        public Tuple2<String, Integer> call(String s) {
                                String [] splited = s.split(" ");

                                return new Tuple2<String, Integer>(splited[0] + "," + splited[1], Integer.valueOf(splited[2]));
                        }
                });

                JavaRDD<String> b_rdd = spark.read().textFile(args[1]).javaRDD();

                JavaPairRDD<String, Integer> b_rdd2 = b_rdd.mapToPair(new PairFunction<String, String, Integer> () {
                        public Tuple2<String, Integer> call(String s) {
                                String [] splited = s.split(" ");

                                return new Tuple2<String, Integer>(splited[0] + "," + splited[1], Integer.valueOf(splited[2]));
                        }
                });

                JavaPairRDD<String, Integer> union_rdd = a_rdd2.union(b_rdd2);

                JavaPairRDD<String, Integer> result_rdd = union_rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                            return i1 + i2;
                    }
            });

            result_rdd.saveAsTextFile(args[2]);

            spark.stop();
    }
}

