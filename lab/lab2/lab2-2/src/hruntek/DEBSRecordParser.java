
package hruntek;

class DEBSRecordParser {

	private static final double LONG_MIN = -74.913585;
	private static final double LAT_MIN = 40.1274702;

	private static final double GRID_LENGTH = 0.011972;
	private static final double GRID_WIDTH = 0.008983112;

	int time;
	double price;
	int x;
	int y;

	void parse(String record) throws Exception {
		String[] splitted = record.split(",");

		price = Double.parseDouble(splitted[16]);
		time = Integer.parseInt(splitted[2].substring(11, 13));

		double pickupLongitude = Double.parseDouble(splitted[6]);
		double pickupLatitude = Double.parseDouble(splitted[7]);

		x = (int) ((pickupLongitude - LONG_MIN) / GRID_LENGTH) + 1;
		y = (int) ((pickupLatitude - LAT_MIN) / GRID_WIDTH) + 1;
	}

}
