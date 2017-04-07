package hruntek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.concurrent.TimeUnit;

public class Hruntek {

	public static final String FILTER_COUNTER = "hruntek.filter.counter";

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - lab2 - task1");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HruntekMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		long startTime = System.nanoTime();
		int status = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.nanoTime();

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

		System.out.println("Time elapsed: " + elapsed + " ms");
		for (Counter c : job.getCounters().getGroup(FILTER_COUNTER)) {
			if (c.getName().equals(FILTER_COUNTER)) {
				System.out.printf("%d rows filtered out%n", c.getValue());
				break;
			}
		}

		System.exit(status);
	}

}
