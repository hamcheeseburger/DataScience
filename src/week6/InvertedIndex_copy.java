package week6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.w3c.dom.Text;

import week6.InvertedIndex.InvertedIndexMapper;
import week6.InvertedIndex.InvertedIndexReducer;

public class InvertedIndex_copy {
	
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private String filename;
		private Text word = new Text();
		private Text index = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			long offset = ((LongWritable) key).get();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			long local_offset = 0;
			while(tokenizer.hasMoreTokens()) {
				String w = tokenizer.nextToken();
				word.set(w);
				
				index.set(filename + " : " + (offset + local_offset));
				
				context.write(word, index);
				
				local_offset += (w.length() + 1);
			}
		}
	
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
	{
		private Text index = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			String v = "";
			while(Text value : values) {
				v += value.toString() + " ";
			}
			index.set(v);
			context.set(key, v);
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
