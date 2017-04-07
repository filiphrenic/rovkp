package hruntek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.concurrent.TimeUnit;

public class Hruntek {

	public static final String LINE_COUNTER = "hruntek.line.counter";

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - hw1 - task1");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HruntekMapper.class);
		if (args.length > 2) {
			job.setCombinerClass(HruntekReducer.class);
		}
		job.setReducerClass(HruntekReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HruntekWritable.class);

		long startTime = System.nanoTime();
		int status = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.nanoTime();

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

		System.out.println("Time elapsed: " + elapsed + " ms");
		for (Counter c : job.getCounters().getGroup(LINE_COUNTER)) {
			if (c.getName().equals(LINE_COUNTER)) {
				System.out.printf("File has %d lines%n", c.getValue());
				break;
			}
		}

		System.exit(status);
	}

}
