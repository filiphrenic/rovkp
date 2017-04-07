package hruntek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Hruntek {

	public static final String INNER_CITY = "hruntek.inner.city";
	public static final String OUTER_CITY = "hruntek.outer.city";
	public static final String PASSENGER_1 = "hruntek.passenger.1";
	public static final String PASSENGER_23 = "hruntek.passenger.2-3";
	public static final String PASSENGER_4 = "hruntek.passenger.4+";
	public static final String TOTAL = "hruntek.total";

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - hw2 - task2");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HruntekMapper.class);
		job.setPartitionerClass(HruntekPartitioner.class);
		job.setReducerClass(HruntekReducer.class);
		job.setNumReduceTasks(6);

		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		long startTime = System.nanoTime();
		int status = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.nanoTime();

		summary(job, INNER_CITY, "Inner city");
		summary(job, OUTER_CITY, "Outer city");

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

		System.out.println("Time elapsed: " + elapsed + " ms");

		System.exit(status);
	}

	public static String getGroup(int key) {
		return key < 3 ? INNER_CITY : OUTER_CITY;
	}

	public static String getType(int key) {
		switch (key % 3) {
			case 0:
				return PASSENGER_1;
			case 1:
				return PASSENGER_23;
			default:
				return PASSENGER_4;
		}
	}

	private static void summary(Job job, String group, String type) throws IOException {
		System.out.printf("Type: %s%n", type);
		for (Counter c : job.getCounters().getGroup(group)) {
			String prefix;
			switch (c.getName()) {
				case PASSENGER_1:
					prefix = "Number of rides with 1 passenger";
					break;
				case PASSENGER_23:
					prefix = "Number of rides with 2-3 passengers";
					break;
				case PASSENGER_4:
					prefix = "Number of rides with 4+ passengers";
					break;
				default: // total
					prefix = "Total number of rides";
			}
			System.out.printf("\t%s:\t%d%n", prefix, c.getValue());
		}
	}
}
