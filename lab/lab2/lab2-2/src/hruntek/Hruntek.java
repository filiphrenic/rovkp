package hruntek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.concurrent.TimeUnit;

public class Hruntek {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Hruntek <input path> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Hruntek.class);
		job.setJobName("Hruntek - lab2 - task2");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HruntekMapper.class);
		job.setCombinerClass(HruntekCombiner.class);
		job.setPartitionerClass(HruntekPartitioner.class);
		job.setReducerClass(HruntekReducer.class);

		job.setNumReduceTasks(24);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Stats.class);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(Text.class);

		long startTime = System.nanoTime();
		int status = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.nanoTime();

		long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
		System.out.println("Time elapsed: " + elapsed + " ms");

		System.exit(status);
	}

}
