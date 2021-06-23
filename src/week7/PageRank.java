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

public class PageRank {
	
	public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
		private IntWritable one_key = new IntWritable();
		private DoubleWritable one_value = new DoubleWritable();
		private int n_pages;
		private double[] pagerank;
		
//		key는 line의 offset, value는 텍스트 파일의 line 한 줄
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int n_links = itr.countTokens() -1;
			if ( n_links == 0 ) return;
//			해당 페이지 id
			int src_id = Integer.parseInt(itr.nextToken().trim());
//			pr = 기여분
			double pr = pagerank[src_id] / (double) n_links;
			one_value.set( pr );
			
			// 연결된 page에 page rank값(기여도) 을 분배
			while(itr.hasMoreTokens()) {
				int linked_id = Integer.parseInt(itr.nextToken().trim());
				one_key.set(linked_id);
				context.write(one_key, one_value);
			}
		}
		
//		Mapper class가 만들어 질 때 최초의 한 번만 실행함!!
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1);
			pagerank = new double[n_pages];
			for(int i = 0; i < n_pages; i++){
				pagerank[i] = conf.getFloat("pagerank" + i, 0 );
			}
		}
	}
	
	public static class PageRankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private double damping_factor = 0.85;
		private double random_factor = 0.15;
		private int n_pages;
		
//		key = 특정 페이지, values = 해당 페이지에 대한 기여도 list
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException
		{
			// 새로운page rank 값구하기.
			double random_incomming_value = random_factor / n_pages;
			double link_incomming_value = 0;
			
//			해당 페이지에 대한 기여도 list 합
			for(DoubleWritable val : values) {
				link_incomming_value += val.get();
			}
			link_incomming_value *= damping_factor;
			
			double agg_val = random_incomming_value + link_incomming_value;
			result.set( agg_val );
			context.write(key, result);
		}
		
//		Reducer class가 만들어 질 때 최초의 한 번만 실행함!!
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1);
		}
	}
	
	public static void initPageRank(Configuration conf, int n_pages) {
//		"1 / 총 페이지갯수" 로 초기화
		conf.setInt("n_pages", n_pages);
		for(int i=0; i < n_pages; i++)
			conf.setFloat("pagerank"+i, (float)(1.0/(double)n_pages));
	}
	
	public static void updatePageRank( Configuration conf, int n_pages) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
//		reduce의 결과를 읽어옴
		Path filenamePath = new Path( "/user/hyeonji/output/part-r-00000" );
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader( new InputStreamReader(in) );
		String line = reader.readLine();
		while( line != null )
		{
			StringTokenizer itr = new StringTokenizer(new String( line ) ) ;
			int src_id = Integer.parseInt(itr.nextToken().trim());
			double pr = Double.parseDouble(itr.nextToken().trim());
//			configuration 객체에 갱신된 rank값 부여
			conf.setFloat( "pagerank"+src_id, (float) pr );
			line = reader.readLine();
		}
	}
	
	public static void main(String[] args) throws Exception {
		int n_pages = 4;
		int n_iter = 3;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: PageRank <in> <out>");
			System.exit(2);
		}
		
		initPageRank(conf, n_pages);
		
//		원래는 이전 rank값과 현재 rank값의 차이가 특정 값 이하일 떄 까지 loop를 돌려야 함
		for( int i = 0; i < n_iter; i++ )
		{
			Job job = new Job(conf, "page rank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);
//			OutputFormatClass TextOutputFormat으로 지정 안함..
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
			
			job.waitForCompletion(true);
			
			// 다음 실행을 위해서 page rank 값 update
			updatePageRank(conf, n_pages);
		}
	}
}
