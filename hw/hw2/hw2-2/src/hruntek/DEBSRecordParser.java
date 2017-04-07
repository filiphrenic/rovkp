package hruntek;

public class DEBSRecordParser {

	private static final double LONG_MIN = -74.0;
	private static final double LONG_MAX = -73.95;
	private static final double LAT_MIN = 40.75;
	private static final double LAT_MAX = 40.8;

	int key;

	public void parse(String record) throws Exception {
		String[] splitted = record.split(",");
		double pickupLongitude = Double.parseDouble(splitted[10]);
		double pickupLatitude = Double.parseDouble(splitted[11]);
		double dropOffLongitude = Double.parseDouble(splitted[12]);
		double dropOffLatitude = Double.parseDouble(splitted[13]);

		key = inRange(pickupLongitude, LONG_MIN, LONG_MAX) &&
				inRange(pickupLatitude, LAT_MIN, LAT_MAX) &&
				inRange(dropOffLongitude, LONG_MIN, LONG_MAX) &&
				inRange(dropOffLatitude, LAT_MIN, LAT_MAX) ? 0 : 3;

		switch (Integer.parseInt(splitted[7])) {
			case 1:
				key += 0;
				break;
			case 2:
			case 3:
				key += 1;
				break;
			default:
				key += 2;
		}
	}

	private static boolean inRange(double x, double min, double max) {
		return x >= min && x <= max;
	}

}
