import java.text.ParseException;

class PollutionReading {


	private int ozone;
	private double longitude;
	private double latitude;

	static PollutionReading create(String line) {
		try {
			return new PollutionReading(line);
		} catch (Throwable t) {
			return null;
		}
	}

	private PollutionReading(String line) throws ParseException {
		String[] split = line.split(",");
		ozone = Integer.parseInt(split[0]);
		longitude = Double.parseDouble(split[5]);
		latitude = Double.parseDouble(split[6]);
	}

	int getOzone() {
		return ozone;
	}

	int getStationID() {
		// TODO
		return -1;
	}

}
