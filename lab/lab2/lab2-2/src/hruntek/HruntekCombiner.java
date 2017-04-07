package hruntek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HruntekCombiner extends Reducer<IntWritable, Stats, IntWritable, Stats> {

	@Override
	protected void reduce(IntWritable key, Iterable<Stats> values, Context context) throws IOException, InterruptedException {
		Stats stats = new Stats();
		values.forEach(stats::combine);
		context.write(key, stats);
	}
}
