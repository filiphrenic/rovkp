package hruntek;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HruntekReducer extends Reducer<VIntWritable, Text, NullWritable, Text> {

	@Override
	protected void reduce(VIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String group = Hruntek.getGroup(key.get());
		Counter total = context.getCounter(group, Hruntek.TOTAL);
		Counter type = context.getCounter(group, Hruntek.getType(key.get()));
		for (Text v : values) {
			total.increment(1L);
			type.increment(1L);
			context.write(NullWritable.get(), v);
		}
	}
}
