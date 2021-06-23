package week11;

import java.util.Arrays;
import java.util.Iterator;

public class SparkWordCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark 
			= SparkSession.builder()
							.appName("SparkWordCount")
							.getOrCreate();
		
		JavaRDD<String> rdd = spark.read().textFile(args[0]).javaRDD();
		
		JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});
		
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		});
		
		JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a1, Integer a2) {
				return a1 + a2;
			}
		});
		
		JavaRDD<String> resultRdd = rdd4.map(x -> x._1 + " " + x._2);
		
		resultRdd.saveAsTextFile(args[1]);
		spark.stop();
	}

}
