
package hruntek;

public class DEBSRecordParser {

	private static final double LONG_MIN = -74.913585;
	private static final double LONG_MAX = -73.117785;
	private static final double LAT_MIN = 40.1274702;
	private static final double LAT_MAX = 41.474937;

	boolean satisfies;

	public void parse(String record) throws Exception {
		String[] splitted = record.split(",");

		double price = Double.parseDouble(splitted[16]);
		if (price > 0) {
			double pickupLongitude = Double.parseDouble(splitted[6]);
			double pickupLatitude = Double.parseDouble(splitted[7]);
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
