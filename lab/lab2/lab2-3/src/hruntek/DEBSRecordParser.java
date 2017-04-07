
package hruntek;

public class DEBSRecordParser {

	private static final double LONG_MIN = -74.913585;
	private static final double LONG_MAX = -73.117785;
	private static final double LAT_MIN = 40.1274702;
	private static final double LAT_MAX = 41.474937;

	private static final double GRID_LENGTH = 0.008983112;
	private static final double GRID_WIDTH = 0.011972;

	public int time;
	public double price;
	public int x;
	public int y;
	public boolean satisfies;

	public void parse(String record) throws Exception {
		String[] splitted = record.split(",");

		price = Double.parseDouble(splitted[16]);
		time = Integer.parseInt(splitted[2].substring(11, 13));

		double pickupLongitude = Double.parseDouble(splitted[6]);
		double pickupLatitude = Double.parseDouble(splitted[7]);

		x = (int) ((pickupLongitude - LONG_MIN) / GRID_LENGTH) + 1;
		y = (int) ((pickupLatitude - LAT_MIN) / GRID_WIDTH) + 1;

		if (price > 0) {
			double dropOffLongitude = Double.parseDouble(splitted[8]);
			double dropOffLatitude = Double.parseDouble(splitted[9]);

			satisfies = inRange(pickupLongitude, LONG_MIN, LONG_MAX) &&
					inRange(pickupLatitude, LAT_MIN, LAT_MAX) &&
					inRange(dropOffLongitude, LONG_MIN, LONG_MAX) &&
					inRange(dropOffLatitude, LAT_MIN, LAT_MAX);
		} else {
			satisfies = false;
		}
	}

	private static boolean inRange(double x, double min, double max) {
		return x >= min && x <= max;
	}

}
