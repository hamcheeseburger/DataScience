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

// Composite Key
class DoubleString implements WritableComparable {
	String joinKey = new String();
	String x = new String();
	public DoubleString() {}
	public DoubleString( String _joinKey, String _x ) {
		joinKey = _joinKey;
		x = _x;
	}
	public void readFields(DataInput in) throws IOException {
		joinKey = in.readUTF();
		x = in.readUTF();
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(x);
	}
		
	// secondary sorting!!
	public int compareTo(Object o1) {
		DoubleString o = (DoubleString) o1;
		int ret = joinKey.compareTo( o.joinKey );
		if (ret!=0) return ret;
		// sorting
		return x.compareTo( o.x );
	}
	public String toString() { 
		return joinKey + " " + x; 
	}

}

public class MatrixMulComKey {
	// WritableComparator
	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result) {
				result = k1.x.compareTo(k2.x);
			}
			return result;
			}
	}
	
	// Partitioner
	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.joinKey.hashCode()%numPartition;
		}
	}
	
	// GroupComparator
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
	
	public static class MatrixMulComKeyMapper extends Mapper<Object, Text, DoubleString, IntWritable> {
		private IntWritable i_value = new IntWritable();
		private int m_value;
		private int k_value;
		private int n_value;
		private boolean isA = false;
		private boolean isB = false;
		
		public void map(Object key, Text value, Context context)
		       	throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if(itr.countTokens() < 3) return;
			DoubleString outputKey = null;
			// if A_matrix row_id(i) col_id(x)
			// if B_matrix row_id(x) col_id(j)
			int row_id = Integer.parseInt( itr.nextToken().trim() ); 
			int col_id = Integer.parseInt( itr.nextToken().trim() ); 
			int matrix_value = Integer.parseInt( itr.nextToken().trim() );
			i_value.set( matrix_value );
			
			if ( isA ) {
				for( int i = 0 ; i < n_value ; i++ ) {
					// i, j, x sequence
					outputKey = new DoubleString(row_id + " " + i, String.valueOf(col_id));
					context.write(outputKey, i_value);
				}
			} else if ( isB ) {
				for( int i = 0; i < m_value ; i++ ) {
					// i, j, x sequence
					outputKey = new DoubleString(i + " " + col_id, String.valueOf(row_id));
					context.write(outputKey, i_value);
				}
			}
			
		}
		
		protected void setup(Context context)
		       	throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			m_value = conf.getInt("m", -1);
			k_value = conf.getInt("k", -1);
			n_value = conf.getInt("n", -1);
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if( filename.equals( "matrix_a.txt" ) ) isA = true;
			if( filename.equals( "matrix_b.txt" ) ) isB = true;
		}
	}
	
	public static class MatrixMulComKeyReducer extends Reducer <DoubleString,IntWritable,Text,IntWritable> {
		public void reduce(DoubleString key, Iterable<IntWritable> values, Context context)
		       	throws IOException, InterruptedException {
			Text reduce_key = new Text(key.joinKey);
			IntWritable reduce_value = new IntWritable();
			
			int i = 1;
			int mulVal = 1;
			int sum = 0;
			for(IntWritable val : values) {
				mulVal *= val.get();
				
				if(i != 1 && i % 2 == 0) {
					sum += mulVal;
					mulVal = 1;
				}
				
				i++;
			}
			
			reduce_value.set(sum);
			context.write(reduce_key, reduce_value);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
        // m is matrix_a's rows number
        // n is matrix_b's cols number
        // k is matrix_a's cols number, matrix_b's rows number
        int m_value = 2;
        int k_value = 2;
        int n_value = 2;
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2)
        {
                System.err.println("Usage: MatrixMul <in> <out>");
                System.exit(2);
        }
        conf.setInt("m", m_value);
        conf.setInt("k", k_value);
        conf.setInt("n", n_value);

        Job job1 = new Job(conf, "matrix mult1");
        job1.setJarByClass(MatrixMulComKey.class);
        job1.setMapperClass(MatrixMulComKeyMapper.class);
        job1.setReducerClass(MatrixMulComKeyReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(DoubleString.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setPartitionerClass(FirstPartitioner.class);
        job1.setGroupingComparatorClass(FirstGroupingComparator.class);
        job1.setSortComparatorClass(CompositeKeyComparator.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        FileSystem.get(job1.getConfiguration()).delete( new Path(otherArgs[1]), true);
        job1.waitForCompletion(true);

		
	}
}
