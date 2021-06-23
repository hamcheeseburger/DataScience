import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;

public class SparkMatrix {
	public static void main(String [] args) throws Exception {
		if(args.length < 1) {
            System.out.println("Usage : SparkMatrix <file>");
            System.exit(1);
		}

		SparkSession spark = SparkSession
            .builder()
            .appName("SparkMatrix")
            .getOrCreate();
		
		JavaRDD<String> mat1 = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> mat2 = spark.read().textFile(args[1]).javaRDD();
		int m = Integer.parseInt(args[2]);
		int k = Integer.parseInt(args[3]);
		int n = Integer.parseInt(args[4]);
		
		JavaPairRDD<String, Integer> m1elements = mat1.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String s) {
				String [] splited = s.split(" ");
				String i = splited[0];
				String x = splited[1];
				Integer value = Integer.valueOf(splited[2]);
			
				String outputkey = "";	
				ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();

				for (int j = 0; j < n; j++) {
					outputkey = i + "," + j + "," + x;
					Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(outputkey, value);
					
					result.add(tuple);
				}

				return result.iterator();
			}
		});
		
		JavaPairRDD<String, Integer> m2elements = mat2.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String s) {
				String [] splited = s.split(" ");
				String x = splited[0];
				String j = splited[1];
				Integer value = Integer.valueOf(splited[2]);
				
				String outputkey = "";
				ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();

				for (int i = 0; i < m; i++) {
					outputkey = i + "," + j + "," + x;
					Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(outputkey, value);
					
					result.add(tuple);
				}

				return result.iterator();
			}
		});
		
		// 두JavaPairRDD를하나의JavaPairRDD로합치기
		JavaPairRDD<String, Integer> elements = m1elements.union(m2elements);
		
		JavaPairRDD<String, Integer> mul= elements.reduceByKey(new Function2<Integer, Integer, Integer> (){
			public Integer call (Integer val1, Integer val2) {
				return val1 * val2;
			}
		});
		
		JavaPairRDD<String, Integer> changeKey= mul.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tp) {
				// key를새롭게만들어서return
				// tip. Tuple2에서 key는 Tuple2._1, value는 Tuple2._2를 사용하여꺼낼수있음
				String key = tp._1();
				Integer val = tp._2();
				String [] splited = key.split(",");
				// i, j만 추출
				String new_key = splited[0] + "," + splited[1];
				
				new Tuple2<String, Integer>(new_key, val);	
			}
		});
		
		JavaPairRDD<String, Integer> rst= changeKey.reduceByKey(new Function2<Integer, Integer, Integer> () {
			public Integer call (Integer val1, Integer val2) {
				return val1 + val2;
			}
		});
		
		rst.saveAsTextFile(args[args.length-1]);
		spark.stop();
	}
}
