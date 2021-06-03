import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class IMDBStudent20170953 {
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] splt = value.toString().split("::");
			
			if(splt.length < 3) return;
			
			String genre = splt[splt.length - 1];
			
			StringTokenizer itr = new StringTokenizer(genre, "|");
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable count = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(LongWritable val : value) {
				sum += val.get();
			}
			
			count.set(sum);
			context.write(key, count);
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
    	job.setOutputValueClass(LongWritable.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));	
    	
    	FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
    	
    	job.waitForCompletion(true);
    }
}
