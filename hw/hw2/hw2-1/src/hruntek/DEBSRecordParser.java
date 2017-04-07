package hruntek;

public class DEBSRecordParser {

	private String medallion;
	private double time;

	public void parse(String record) throws Exception {
		String[] splitted = record.split(",");
		medallion = splitted[0];
		time = Double.parseDouble(splitted[8]);

	}

	public String getMedallion() {
		return medallion;
	}

	public double getTime() {
		return time;
	}

}
