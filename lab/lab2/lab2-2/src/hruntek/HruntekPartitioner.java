package hruntek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HruntekPartitioner extends Partitioner<IntWritable, Stats> {

	@Override
	public int getPartition(IntWritable key, Stats value, int numPartitions) {
		return Util.time(key.get());
	}
}
