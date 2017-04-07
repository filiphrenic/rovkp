package hruntek.first;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReducer extends Reducer<VIntWritable, Text, NullWritable, Text> {

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text v : values) {
			context.write(NullWritable.get(), v);
		}
	}
}
