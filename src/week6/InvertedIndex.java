package week6;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
	
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private String filename;
		private static final int SPACE_COUNT = 1;
		private Text word = new Text();
		private Text one_value = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			long byte_offset = ((LongWritable) key).get();
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			long word_offset = 0;
			while(itr.hasMoreTokens()) {
				String word = itr.nextToken();
				
				long total_offset = byte_offset + word_offset;
				context.write(new Text(word), new Text(filename + ":" + total_offset));
				
				word_offset += (word.length() + SPACE_COUNT);
			}
		}
	
		protected void setup(Context contex) 
				throws IOException, InterruptedException {
			filename = ((FileSplit) contex.getInputSplit()).getPath().getName();
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				Text result = new Text();
				StringBuffer values_buffer = new StringBuffer();
				for (Text val : values) {
					values_buffer.append( val.toString() );
					values_buffer.append(" ");
				}
				result.set(values_buffer.toString());
				context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "inverted index");
		
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}

}
