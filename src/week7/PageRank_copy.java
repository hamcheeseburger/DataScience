package week7;

import java.io.BufferedReader;
import java.io.FileSystem;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.w3c.dom.Text;

import week7.PageRank.PageRankMapper;
import week7.PageRank.PageRankReducer;

public class PageRank_copy {
	public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		private IntWritable page = new IntWritable();
		private DoubleWritable rank = new DoubleWritable();
		private int page_n;
		private double [] prValues;
		
		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);
			
//			Source Page가 포함하고 있는 링크의 갯수
			int links_n = st.countTokens() - 1;
			if(links_n == 0) return;
			
			int source_id = Integer.parseInt(st.nextToken().trim());
			double source_rank = prValues[source_id] / (double) links_n;
			rank.set(source_rank);
			
			while(st.hasMoreTokens()) {
				int target_id = Integer.parseInt(st.nextToken());
				page.set(target_id);
				
				context.write(page, rank);
			}
			
		}
	
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			page_n = conf.getInt("page_n", -1);
			
			prValues = new double[page_n];
			for(int i = 0; i < page_n; i++) {
				Double d = conf.getFloat("pageRank" + i, 0);
				prvalues[i] = d;
			}
		}
	}
	
	public static class PageRankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		private int page_n;
		private static final double dumping_factor = 0.85;
		private static final double random_factor = 1 - dumping_factor;
		private DoubleWritable pr = new DoubleWrtiable();
		
		public void reduce(IntWritable key, Itarable<DoubleWritable> values) {
			double sum = 0.0;
			for(DoubleWritable d : values) {
				sum += d.get();
			}
			
			pr.set(dumping_factor * sum + random_factor * page_n);
			
			context.write(key, pr);
		}
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			page_n = conf.getInt("page_n", 0);
		}
	}
	
	
	public static void initPageRank(Configuration conf, int n_pages) {
//		"1 / 총 페이지갯수" 로 초기화
		conf.setInt("n_pages", n_pages);
		for(int i=0; i < n_pages; i++)
			conf.setFloat("pagerank"+i, (float)(1.0/(double)n_pages));
	}
	
	public static void updatePageRank(Configuration conf, int n_pages) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
//		reduce의 결과를 읽어옴
		Path filenamePath = new Path("/user/hyeonji/output/part-r-00000" );
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader( new InputStreamReader(in) );
		String line = reader.readLine();
		while( line != null )
		{
			StringTokenizer itr = new StringTokenizer(new String( line )) ;
//			page Id
			int src_id = Integer.parseInt(itr.nextToken().trim());
//			pageRank 값
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
		
//		원래는 이전 rank값과 현재 rank값의 차이가 특정 값 이하일 때 까지 loop를 돌려야 함
		for( int i = 0; i < n_iter; i++ )
		{
			Job job = new Job(conf, "page rank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
			job.waitForCompletion(true);
			// 다음 실행을 위해서 page rank 값 update
			updatePageRank(conf, n_pages);
		}
	}
}
