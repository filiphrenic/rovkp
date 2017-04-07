package hruntek.first;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<VIntWritable, Text> {

	@Override
	public int getPartition(VIntWritable vIntWritable, Text text, int i) {
		return vIntWritable.get();
	}
}
