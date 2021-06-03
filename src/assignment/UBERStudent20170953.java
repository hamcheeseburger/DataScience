import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;

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
	
	public static class Map extends Mapper <LongWritable, Text, Text, Text> {
		Text word = new Text();
		Text val = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			if(itr.countTokens() < 4) return;
			
			String base_number =  itr.nextToken().trim();
			String date = itr.nextToken().trim(); 
			int active_vehicles = Integer.parseInt( itr.nextToken().trim() );
			int trips = Integer.parseInt( itr.nextToken().trim() );
			
			String dayOfWeek = "";
			try {
				SimpleDateFormat transFormat = new SimpleDateFormat("dd/MM/yyyy");
				Date dateObj = transFormat.parse(date);
			
				Calendar cal = Calendar.getInstance() ;
				cal.setTime(dateObj);
			     
				int dayNum = cal.get(Calendar.DAY_OF_WEEK) ;
				dayOfWeek = parseToString(dayNum);
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			word.set(base_number + "," + dayOfWeek);
			val.set(trips + "," + active_vehicles);
			
			context.write(word, val);
		}
		
	}
	
	public static class Reduce extends Reducer <Text, Text, Text, Text> {
		Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			int total_active_vehicles = 0;
			int total_trips = 0;
			
			for(Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				int trips = Integer.parseInt( itr.nextToken().trim() );
				int active_vehicles = Integer.parseInt( itr.nextToken().trim() );
				
				total_active_vehicles += active_vehicles;
				total_trips += trips;
			}
			
			result.set(total_trips + "," + total_active_vehicles);
			
			context.write(key, result);
		}
	}
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
    	
    	Job job = new Job(conf, "IMDBStudent20170953");
    	job.setJarByClass(IMDBStudent20170953.class);
    	
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    	job.setCombinerClass(Reduce.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
    	FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

    	job.waitForCompletion(true);
    }
}
