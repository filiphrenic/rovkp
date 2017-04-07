package hruntek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HruntekReducer extends Reducer<IntWritable, Stats, VIntWritable, Text> {

	private VIntWritable time;

	private int xRide, yRide;
	private int maxRides;

	private int xPrice, yPrice;
	private double maxPrice;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		time = null;
		maxRides = Integer.MIN_VALUE;
		maxPrice = Double.MIN_VALUE;
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Stats> values, Context context) throws IOException, InterruptedException {
		int code = key.get();

		if (time == null) {
			time = new VIntWritable(Util.time(code));
		}

		for (Stats s : values) {
			if (maxRides < s.numRides.get()) {
				maxRides = s.numRides.get();
				xRide = Util.x(code);
				yRide = Util.y(code);
			}
			if (maxPrice < s.maxPrice.get()) {
				maxPrice = s.maxPrice.get();
				xPrice = Util.x(code);
				yPrice = Util.y(code);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String text = String.format("\n[%d.%d]\t%d\n[%d.%d]\t%.2f", xRide, yRide, maxRides, xPrice, yPrice, maxPrice);
		context.write(time, new Text(text));
	}
}
