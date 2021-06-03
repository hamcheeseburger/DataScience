import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import week10.Context;
import week10.Emp;
import week10.Text;


public class KmeansCombiner {
	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable one_key = new IntWritable();
		private int n_centers;
		private double[] center_x;
		private double[] center_y;
		
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			n_centers = conf.getInt("n_centers", -1);
			center_x = new double[n_centers];
			center_y = new double[n_centers];
			for(int i=0; i<n_centers; i++) {
				center_x[i] = conf.getFloat( "center_x_"+i , 0 );
				center_y[i] = conf.getFloat( "center_y_"+i , 0 );
			}
		}
		
		public double getDist(double x1, double y1, double x2, double y2) {
			double dist = (x1-x2)* (x1-x2) + (y1-y2)*(y1-y2);
			return Math.sqrt( dist );
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			if( itr.countTokens() < 2 ) return;
			if( n_centers == 0 ) return;
			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			int cluster_idx = 0;
			
			double min_distance = getDist(x, y, center_x[0], center_y[0]);
			for (int i = 1; i < center_x.length; i++) {
				double distance = getDist(x, y, center_x[i], center_y[i]);
				if(distance < min_distance) {
					min_distance = distance;
					cluster_idx = i;
				}
			}
			one_key.set( cluster_idx );
			
			context.write( one_key, value );
		}
	}
	
	
	public static class KMeansReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		int n_centers;
		int [] val_total;
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			Text result = new Text();
			for (Text val : values) {
				// calculate center
				String xy = val.toString();
				String [] splited = xy.split(",");
				double x = Double.parseDouble(splited[0]);
				double y = Double.parseDouble(splited[1]);
				
				x_total += x;
				y_total += y;
				cnt++;
			}

			result.set((x_total / cnt) + "," + (y_total / cnt));
			
			context.write(key, result);
		}
		
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			n_centers = conf.getInt("n_centers", -1);
			val_total = new Integer[n_centers];
			for(int i=0; i<n_centers; i++) {
				val_total[i] = conf.getFloat( "val_"+i , 0 );
			}
		}
	}
	
	public static void initCenter(Configuration conf, int n_centers) {
		conf.setInt("n_centers", n_centers);
		for(int i=0; i < n_centers; i++) {
			conf.setFloat("center_x_"+i, (float)(1.0/(double)n_centers));
			conf.setFloat("center_y_"+i, (float)(1.0/(double)n_centers));
		}
	}
	
	public static void updateCenter (Configuration conf) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path( "/user/hyeonji/kmeansoutput/part-r-00000" );
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader( new InputStreamReader(in) );
		String line = reader.readLine();
		while( line != null )
		{
			StringTokenizer itr = new StringTokenizer(new String( line ) ) ;
			int center_id = Integer.parseInt(itr.nextToken().trim());
			String xy = itr.nextToken().trim();
			String [] splited = xy.split(",");
			double x = Double.parseDouble(splited[0]);
			double y = Double.parseDouble(splited[1]);

			conf.setFloat( "center_x_"+center_id, (float) x );
			conf.setFloat( "center_y_"+center_id, (float) y );
			line = reader.readLine();
		}
	}
	
	public static void main(String[] args) throws Exception {
		int n_iter = 3;
		int n_centers = 2;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Kmeans <in> <out>");
			System.exit(2);
		}
		
		initCenter(conf, n_centers);
		
		for( int i = 0; i < 1; i++ ) {
			Job job = new Job(conf, "Kmeans");
			job.setJarByClass(KmeansCombiner.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setCombinerClass(KMeansReducer.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
			
			job.waitForCompletion(true);
			
			updateCenter(conf);
		}
	}
}
