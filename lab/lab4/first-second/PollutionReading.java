import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

class PollutionReading {

	private static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private String reading;
	private Date timestamp;

	static PollutionReading create(String line) {
		try {
			return new PollutionReading(line);
		} catch (Throwable t) {
			return null;
		}
	}

	private PollutionReading(String line) throws ParseException {
		String[] split = line.split(",");
		timestamp = new Date();
		timestamp = FORMAT.parse(split[7]);
		reading = line.trim();
	}

	Date getTimestamp() {
		return timestamp;
	}

	String getReading() {
		return reading;
	}
}
