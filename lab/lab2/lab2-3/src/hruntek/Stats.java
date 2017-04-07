package hruntek;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Stats implements Writable {

	public VIntWritable numRides;
	public DoubleWritable maxPrice;

	public Stats(double price) {
		numRides = new VIntWritable(1);
		maxPrice = new DoubleWritable(price);
	}

	public Stats() {
		numRides = new VIntWritable(0);
		maxPrice = new DoubleWritable(0.0);
	}

	public void combine(Stats s) {
		numRides.set(numRides.get() + s.numRides.get());
		maxPrice.set(maxPrice.get() + s.maxPrice.get());
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		numRides.write(dataOutput);
		maxPrice.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		numRides.readFields(dataInput);
		maxPrice.readFields(dataInput);
	}
}
