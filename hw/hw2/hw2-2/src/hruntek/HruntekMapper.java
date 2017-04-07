package hruntek;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HruntekMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		DEBSRecordParser parser = new DEBSRecordParser();

		//skip the first line
		if (key.get() > 0) {
			String record = value.toString();
			try {
				parser.parse(record);
				context.write(new VIntWritable(parser.key), new Text(value));
			} catch (Exception ex) {
				System.out.println("Cannot parse: " + record + "due to the " + ex);
			}
		}
	}
}
