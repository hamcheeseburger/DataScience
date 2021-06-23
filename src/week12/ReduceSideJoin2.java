// solution ver.2
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

// joinKey = 상품코드
// tableName = A or B

// Composite Key
class DoubleString implements WritableComparable {
	String joinKey = new String();
	String tableName = new String();
	public DoubleString() {}
	public DoubleString( String _joinKey, String _tableName ) {
		joinKey = _joinKey;
		tableName = _tableName;
	}
	public void readFields(DataInput in) throws IOException {
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
		
	// effect to order of value list
	public int compareTo(Object o1) {
		DoubleString o = (DoubleString) o1;
		int ret = joinKey.compareTo( o.joinKey );
		if (ret!=0) return ret;
		// reverse sorting
		return -1 * tableName.compareTo( o.tableName );
	}
	public String toString() { 
		return joinKey + " " + tableName; 
	}

}

public class ReduceSideJoin2 {

	// WritableComparator
	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
//			int result = k1.joinKey.compareTo(k2.joinKey);

//			if(0 == result) { // join key가 같으면
//				result = -1* k1.tableName.compareTo(k2.tableName);
//			}
			
			int result = k1.compareTo(k2);
			return result;
		}
	}
	
	
	// Partitioner
	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.joinKey.hashCode() % numPartition;
		}
	}
	
	// GroupComparator => Join Key가 같으면 같은 value list로 들어가도록
	public static class FirstGroupingComparator extends WritableComparator {
		protected FirstGroupingComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}
		
	public static class ReduceSideJoin2Mapper extends Mapper<Object, Text, DoubleString, Text> {
		boolean fileA = true;
		public void map(Object key, Text value, Context context)
			       	throws IOException, InterruptedException {
			System.out.println("ReduceSideJoin2Mapper");
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			DoubleString outputKey = null;
			Text outputValue = new Text();
			
			if( fileA ) {
				String product_id = itr.nextToken();
				String product_price = itr.nextToken();
				String product_code = itr.nextToken();

				outputKey = new DoubleString(product_code, "A");
				outputValue.set(product_id + "," + product_price);
			}
			else {
				String product_code = itr.nextToken();
				String product_desc = itr.nextToken();
				
				outputKey = new DoubleString(product_code, "B");
				outputValue.set(product_desc);
			}
			context.write( outputKey, outputValue );
		}

		protected void setup(Context context)
			       	throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "relation_a" ) != -1 ) fileA = true;
			else fileA = false;
		}
	}

	public static class ReduceSideJoin2Reducer extends Reducer <DoubleString,Text,Text,Text> {
		public void reduce(DoubleString key, Iterable<Text> values, Context context)
			       	throws IOException, InterruptedException {
			System.out.println("ReduceSideJoin2Reducer");
		
			Text reduce_key = new Text();
			Text reduce_val = new Text();
			int i = 0;
			String description = "";
			for(Text val: values) {
				//System.out.println(val.toString());
				if(i == 0) {
					description = val.toString();
				} else {
					String aVal = val.toString();
					String [] splitedVal = aVal.split(",");
					
					String product_id = splitedVal[0];
					String product_price = splitedVal[1];
					System.out.println("product_id" + product_id);
					System.out.println("product_price" + product_price);	
					reduce_key.set(product_id);
					reduce_val.set(product_price + " " + description);
					context.write(reduce_key, reduce_val);
				}
				i++;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReduceSideJoin2 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "ReduceSideJoin2");
		job.setJarByClass(ReduceSideJoin2.class);
		job.setMapperClass(ReduceSideJoin2Mapper.class);
		job.setReducerClass(ReduceSideJoin2Reducer.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
//		같은 join key를 갖는 것들은 같은 reducer로 들어가도록 함
		job.setPartitionerClass(FirstPartitioner.class);
//		Join key가 같은 것끼리 suffling되도록 정의
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
//		Suffling이 된 value_list에서 역순으로 정렬
		job.setSortComparatorClass(CompositeKeyComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
