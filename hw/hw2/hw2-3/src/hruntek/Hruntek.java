package hruntek;

import hruntek.first.FirstMapper;
import hruntek.first.FirstPartitioner;
import hruntek.first.FirstReducer;
import hruntek.second.SecondMapper;
import hruntek.second.SecondReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

	private static final String TEMP_RESULT = "hw2-temp";
	private static final Path TEMP = new Path(TEMP_RESULT);

	public static final String GROUP = "hruntek.group";
	public static final String TYPE = "hruntek.type";

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		FileSystem fs = FileSystem.get(new Configuration(true));

		int status = 1;
		long startTime = System.nanoTime();
		if (createFirstJob(args[0]).waitForCompletion(true)) {
			status = 0;
			for (int i = 0; i < 6; i++) {
				Job second = createSecondJob(i, args[1]);
//				second.submit();
				status = Math.max(status, second.waitForCompletion(false) ? 0 : 1);
				summary(second, getGroup(i), getPrefix(i));
			}
		}
		long endTime = System.nanoTime();

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
		System.out.println("Time elapsed: " + elapsed + " ms");

		fs.delete(TEMP, true);

		System.exit(status);
	}

	private static Job createFirstJob(String input) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - hw2 - task3 - job1");

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, TEMP);

		job.setMapperClass(FirstMapper.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setReducerClass(FirstReducer.class);
		job.setNumReduceTasks(6);

		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

	private static Job createSecondJob(int i, String output) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - hw2 - task3 - job2");

		String input = String.format("%s/part-r-%05d", TEMP_RESULT, i);
		String output_ = String.format("%s/final%d", output, i);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output_));

		job.setMapperClass(SecondMapper.class);
		job.setReducerClass(SecondReducer.class);
		job.setNumReduceTasks(6);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HruntekWritable.class);

		return job;
	}

	private static void summary(Job job, String text, String prefix) throws IOException {
		System.out.printf("Type: %s%n", text);
		for (Counter c : job.getCounters().getGroup(GROUP)) {
			System.out.printf("\t%s:\t%d%n", prefix, c.getValue());
		}
	}

	private static String getGroup(int key) {
		return key < 3 ? "Inner city" : "Outer city";
	}

	private static String getPrefix(int key) {
		switch (key % 3) {
			case 0:
				return "Number of different vehicles with 1 passenger";
			case 1:
				return "Number of different vehicles with 2-3 passengers";
			default:
				return "Number of different vehicles with 4+ passengers";
		}
	}
}
