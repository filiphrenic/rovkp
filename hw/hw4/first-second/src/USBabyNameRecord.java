import java.io.Serializable;

class USBabyNameRecord implements Serializable {
	private int id;
	private String name;
	private int year;
	private boolean male;
	private String state;
	private long count;

	static USBabyNameRecord create(String line) {
		try {
			return new USBabyNameRecord(line);
		} catch (Throwable t) {
			return null;
		}
	}

	private USBabyNameRecord(String line) {
		String[] split = line.split(",");
		id = Integer.parseInt(split[0]);
		name = split[1];
		year = Integer.parseInt(split[2]);

		String gender = split[3];
		if ("M".equals(gender)) {
			male = true;
		} else if ("F".equals(gender)) {
			male = false;
		} else {
			throw new RuntimeException("gender");
		}

		state = split[4];
		count = Integer.parseInt(split[5]);
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public int getYear() {
		return year;
	}

	public boolean isMale() {
		return male;
	}

	public String getState() {
		return state;
	}

	public long getCount() {
		return count;
	}
}