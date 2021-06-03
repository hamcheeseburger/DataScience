import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class Video {
	public String video_genre;
	public double average;
	
	public Video(String video_genre, double average) {
		this.video_genre = video_genre;
		this.average = average;
	}
	
	public String getGenre() {
		return this.video_genre;
	}
	
	public double getAverage() {
		return this.average;
	}
}

public class YouTubeStudent20170953 {
	public static class AverageComparator implements Comparator<Video> {
		public int compare(Video x, Video y) {
			if ( x.average > y.average ) return 1;
			if ( x.average < y.average ) return -1;
			return 0;
		}
	}
	
	public static void insertVideo(PriorityQueue q, String video_genre, double average, int topK) {
		Video video_head = (Video) q.peek();
		if ( q.size() < topK || video_head.average < average ) {
			Video video = new Video(video_genre, average);
			q.add(video);
			
			if(q.size() > topK) q.remove();
		}
		
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String video_genre = "";
			String video_rate = "";
			
			int i = 0;
			while(itr.hasMoreTokens()) {
				video_rate = itr.nextToken();
				if(i == 3) {
					video_genre = video_rate;
				}
				
				i++;
			}
			
			System.out.println("Genre : " + video_genre + " Rate : " + video_rate);
			
			context.write(new Text(video_genre), new DoubleWritable(Double.valueOf(video_rate)));
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private PriorityQueue<Video> queue;
		private Comparator<Video> comp = new AverageComparator();
		private int topK;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		       	throws IOException, InterruptedException {
			double rate_total = 0;
			int size = 0;
			
			for(DoubleWritable val : values) {
				rate_total += val.get();
				size++;
			}
			
			double average = rate_total / size;
			
			insertVideo(queue, key.toString(), average, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Video>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Video video = (Video) queue.remove();
				context.write(new Text(video.getGenre()), new DoubleWritable(video.getAverage()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTubeStudent20170953 <in> <out> <k>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "YouTubeStudent20170953");
		job.setJarByClass(YouTubeStudent20170953.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		job.setNumReduceTasks(1);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
