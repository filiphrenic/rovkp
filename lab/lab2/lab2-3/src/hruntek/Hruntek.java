package hruntek;

import hruntek.second.HruntekCombiner;
import hruntek.second.HruntekMapper;
import hruntek.second.HruntekPartitioner;
import hruntek.second.HruntekReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Hruntek {

	private static final Path TEMP_RESULT = new Path("lab2-temp-r-0");

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		Job first = createFirstJob(args[0]);
		Job second = createSecondJob(args[1]);

		long startTime = System.nanoTime();
		int status = 1;
		if (first.waitForCompletion(true)) {
			status = second.waitForCompletion(true) ? 0 : 1;
		}
		long endTime = System.nanoTime();

		deleteTemp();

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
		System.out.println("Time elapsed: " + elapsed + " ms");

		System.exit(status);
	}

	private static Job createFirstJob(String input) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - lab2 - task3 - job1");

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, TEMP_RESULT);

		job.setMapperClass(hruntek.first.HruntekMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		return job;
	}

	private static Job createSecondJob(String output) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - lab2 - task2 - job2");

		FileInputFormat.addInputPath(job, TEMP_RESULT);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(HruntekMapper.class);
		job.setCombinerClass(HruntekCombiner.class);
		job.setPartitionerClass(HruntekPartitioner.class);
		job.setReducerClass(HruntekReducer.class);

		job.setNumReduceTasks(24);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Stats.class);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

	private static void deleteTemp() throws IOException {
		FileSystem.get(new Configuration(true)).delete(TEMP_RESULT, true);
	}

}
