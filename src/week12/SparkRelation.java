import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;

class Product implements Serializable {
	Integer product_id;
	Integer product_price;
	String product_code;
	public Product (Integer _product_id, Integer _product_price, String _product_code) {
		product_id = _product_id;
		product_price = _product_price;
		product_code = _product_code;
	}
	
	public String toString() {
		return "id : " + product_id + ", price : " + product_price + ", code : " + product_code;
	}
}

class Code implements Serializable {
	String product_code;
	String product_desc;
	
	public Code(String _product_code, String _product_desc) {
		product_code = _product_code;
		product_desc = _product_desc;
	}
	
	public String toString() {
		return "code : " + product_code + ", desc : " + product_desc;
	}
}

public class SparkRelation {
	public static void main(String [] args) {
		 if(args.length < 1) {
             System.out.println("Usage : JavaWordCount <file>");
             System.exit(1);
		 }

    	SparkSession spark = SparkSession
             .builder()
             .appName("SparkRelation")
             .getOrCreate();
    	// read file
    	JavaRDD<String> products = spark.read().textFile(args[0]).javaRDD();
     	// split text file with " 

    	JavaPairRDD<String, Product> pTuples = products.mapToPair(new PairFunction<String, String, Product>() {
    		public Tuple2<String, Product> call(String s) {
    			String [] splited = s.split("|");
    			String key = splited[2];
    			Product product = new Product(Integer.valueOf(splited[0]), Integer.valueOf(splited[1]), key);
    			return new Tuple2(key, product);
    		}

    	});
	
    	JavaRDD<String> codes = spark.read().textFile(args[1]).javaRDD();

    	JavaPairRDD<String, Code> cTuples = codes.mapToPair(new PairFunction<String, String, Code>() {
    		public Tuple2<String, Code> call(String s) {
    			String[] splited = s.split("|");
    			return new Tuple2(splited[0], new Code(splited[0], splited[1]));
    		}
    	});
    	
    	JavaPairRDD<String, Tuple2<Product, Code>> joined = pTuples.join(cTuples);
    	JavaPairRDD<String, Tuple2<Product, Optional<Code>>> leftOuterJoined = pTuples.leftOuterJoin(cTuples);
    	JavaPairRDD<String, Tuple2<Optional<Product>, Code>> rightOuterJoined = pTuples.rightOuterJoin(cTuples);
    	JavaPairRDD<String, Tuple2<Optional<Product>, Optional<Code>>> fullOuterJoined = pTuples.fullOuterJoin(cTuples);
    	
    	joined.saveAsTextFile(args[args.length - 1] + "_join");
    	leftOuterJoined.saveAsTextFile(args[args.length - 1] + "_leftOuterJoin");
    	rightOuterJoined.saveAsTextFile(args[args.length - 1] + "_rightOuterJoin");
    	fullOuterJoined.saveAsTextFile(args[args.length - 1] + "_fullOuterJoin");
    	
    	spark.stop();
	}
}
