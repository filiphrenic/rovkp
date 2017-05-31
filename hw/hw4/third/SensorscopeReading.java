import java.io.Serializable;

class SensorscopeReading implements Serializable {

	private int stationID;
	private double solarPanelCurrent;

	static SensorscopeReading create(String line) {
		try {
			return new SensorscopeReading(line, ",");
		} catch (Throwable t) {
			return null;
		}
	}

	private SensorscopeReading(String line, String regex) {
		String[] split = line.split(regex);
		stationID = Integer.parseInt(split[0]);
		solarPanelCurrent = Double.parseDouble(split[16]);
	}


	int getStationID() {
		return stationID;
	}

	double getSolarPanelCurrent() {
		return solarPanelCurrent;
	}

}
