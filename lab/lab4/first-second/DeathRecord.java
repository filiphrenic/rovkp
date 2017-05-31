class DeathRecord {

	private int monthOfDeath;
	private boolean male;
	private int age;
	private int dayOfWeekOfDeath;
	private boolean autopsy;
	private boolean married;
	private boolean accident;

	static DeathRecord create(String line) {
		try {
			return new DeathRecord(line);
		} catch (Throwable t) {
			return null;
		}
	}

	private DeathRecord(String line) {
		String[] split = line.split(",");

		monthOfDeath = Integer.parseInt(split[5]);
		male = "M".equals(split[6]);

		int ageType = Integer.parseInt(split[7]);
		int age = Integer.parseInt(split[8]);

		if (ageType == 1) {
			this.age = age;
		} else if (ageType == 2) {
			this.age = age / 12;
		} else {
			throw new RuntimeException("age");
		}

		accident = "1".equals(split[19]);
		married = "M".equals(split[15]);
		dayOfWeekOfDeath = Integer.parseInt(split[16]);
		autopsy = "Y".equals(split[21]);
	}

	boolean isMale() {
		return male;
	}

	int getMonthOfDeath() {
		return monthOfDeath;
	}

	int getAge() {
		return age;
	}

	int getDayOfWeekOfDeath() {
		return dayOfWeekOfDeath;
	}

	boolean isAutopsy() {
		return autopsy;
	}

	boolean isMarried() {
		return married;
	}

	boolean isAccident() {
		return accident;
	}
}
