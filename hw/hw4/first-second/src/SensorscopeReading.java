import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

class SensorscopeReading implements Serializable {
	private String reading;
	private long time;

	static SensorscopeReading create(String line) {
		try {
			return new SensorscopeReading(line);
		} catch (Throwable t) {
			return null;
		}
	}

	private SensorscopeReading(String line) {
		String[] split = line.split("\\s+");
		time = Long.parseLong(split[7]);
		reading = Arrays.stream(split).collect(Collectors.joining(","));
	}

	long getTime() {
		return time;
	}

	String getReading() {
		return reading;
	}

}
