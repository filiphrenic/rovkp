package hruntek;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HruntekWritable implements Writable {

	private DoubleWritable total;
	private DoubleWritable min;
	private DoubleWritable max;

	public HruntekWritable(double time) {
		this(time, time, time);
	}

	public HruntekWritable(double total, double min, double max) {
		this.total = new DoubleWritable(total);
		this.min = new DoubleWritable(min);
		this.max = new DoubleWritable(max);
	}

	public HruntekWritable() {
		this(0, Double.MAX_VALUE, Double.MIN_VALUE);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		total.write(dataOutput);
		min.write(dataOutput);
		max.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		total.readFields(dataInput);
		min.readFields(dataInput);
		max.readFields(dataInput);
	}

	public void combine(HruntekWritable hruntek) {
		total.set(getTotal() + hruntek.getTotal());
		min.set(Math.min(getMin(), hruntek.getMin()));
		max.set(Math.max(getMax(), hruntek.getMax()));
	}

	public double getTotal() {
		return total.get();
	}

	public double getMin() {
		return min.get();
	}

	public double getMax() {
		return max.get();
	}

	@Override
	public String toString() {
		return String.format("[total=%.2f\tmin=%.2f\tmax=%.2f]", getTotal(), getMin(), getMax());
	}
}
