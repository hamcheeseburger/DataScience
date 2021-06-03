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
				String row = splited[0];
				String col = splited[1];
				Integer value = Integer.valueOf(splited[2]);
			
				String outputkey = "";	
				ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
				// return�� element��������ArrayList�����
				for (int i = 0; i < n; i++) {
					outputkey = row + "," + i + "," + col;
					Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(outputkey, value);
					
					result.add(tuple);
				}
				// matrix_a�� �´� ������ index ���� ArrayList�� add �ϱ�
				//ArrayList��iterator��return
				return result.iterator();
			}
		});
		JavaPairRDD<String, Integer> m2elements = mat2.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String s) {
				String [] splited = s.split(" ");
				String row = splited[0];
				String col = splited[1];
				Integer value = Integer.valueOf(splited[2]);
				
				String outputkey = "";
				ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
				// return��element��������ArrayList�����
				for (int i = 0; i < m; i++) {
					outputkey = i + "," + col + "," + row;
					Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(outputkey, value);
					
					result.add(tuple);
				}
				// matrix_b�� �´� ������index ����ArrayList��add �ϱ�
				//ArrayList��iterator��return
				
				return result.iterator();
			}
		});
		
		// ��JavaPairRDD���ϳ���JavaPairRDD����ġ��
		JavaPairRDD<String, Integer> elements = m1elements.union(m2elements);
		
		JavaPairRDD<String, Integer> mul= elements.reduceByKey(new Function2<Integer, Integer, Integer> (){
			public Integer call (Integer val1, Integer val2) {
				return val1 * val2;
			}
		});
		
		JavaPairRDD<String, Integer> changeKey= mul.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tp) {
				// key�����ӰԸ���return
				// tip. Tuple2���� key�� Tuple2._1, value�� Tuple2._2�� ����Ͽ�����������
				String key = tp._1();
				Integer val = tp._2();
				String [] splited = key.split(",");
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
