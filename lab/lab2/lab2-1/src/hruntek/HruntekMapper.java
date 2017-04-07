package hruntek;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HruntekMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		DEBSRecordParser parser = new DEBSRecordParser();
		Text text = new Text();

		//skip the first line
		if (key.get() > 0) {
			String record = value.toString();
			try {
				parser.parse(record);
				if (parser.satisfies) {
					text.set(record);
					context.write(NullWritable.get(), text);
				} else {
					context.getCounter(Hruntek.FILTER_COUNTER, Hruntek.FILTER_COUNTER).increment(1L);
				}
			} catch (Exception ex) {
				System.out.println("Cannot parse: " + record + "due to the " + ex);
			}
		}
	}
}
