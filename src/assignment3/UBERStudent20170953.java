import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import scala.Tuple2;

public class UBERStudent20170953 {
	public static String parseToString(int dayNum) {
		switch(dayNum){
        	case 1:
        		return "SUN";
        	case 2:
        		return "MON";
        	case 3:
        		return "TUE";
        	case 4:
        		return "WED";
        	case 5:
        		return "THR";
        	case 6:
        		return "FRI";
        	case 7:
        		return "SAT";
        	default:
        		return "NULL";
		}
	}
	
	public static void main(String[] args) throws Exception {
	
		// TODO Auto-generated method stub
		if(args.length < 1) {
            System.out.println("Usage : UBERStudent20170953 <input> <output>");
            System.exit(1);
		}

		SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20170953")
            .getOrCreate();
		// read file
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		// mapper
		JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
            	String [] splited = s.split(",");
            	String date = splited[1];
                
            	SimpleDateFormat transFormat = new SimpleDateFormat("MM/dd/yyyy");
				String dayOfWeek = "";
				try {
					Date dateObj = transFormat.parse(date);
			
					Calendar cal = Calendar.getInstance() ;
					cal.setTime(dateObj);
			     
					int dayNum = cal.get(Calendar.DAY_OF_WEEK) ;
				
					dayOfWeek = parseToString(dayNum);
				} catch (Exception e) {}
				String key = splited[0] + "," + dayOfWeek;
				String value = splited[3] + "," + splited[2];

				System.out.println("[MAPPER] " + key + " " + value);
				return new Tuple2(key, value);
            }
		});

		JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
            public String call(String val1, String val2) {
                String [] val1_splited = val1.split(",");
                String [] val2_splited = val2.split(",");
                
                int trips = Integer.valueOf(val1_splited[0]) + Integer.valueOf(val2_splited[0]);
                int vehicles = Integer.valueOf(val1_splited[1]) + Integer.valueOf(val2_splited[1]);
       		
	       	System.out.println("[REDUCER] " + trips + "," + vehicles);	
                return trips + "," + vehicles;
        }
	});
		// write result
		counts.saveAsTextFile(args[1]);
    	spark.stop();
	}

}
