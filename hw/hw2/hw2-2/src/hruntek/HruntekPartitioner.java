package hruntek;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HruntekPartitioner extends Partitioner<VIntWritable, Text> {

	@Override
	public int getPartition(VIntWritable vIntWritable, Text text, int i) {
		return vIntWritable.get();
	}
}
