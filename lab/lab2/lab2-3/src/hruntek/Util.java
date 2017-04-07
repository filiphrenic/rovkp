package hruntek;

public class Util {

	private static final int XXX = 150 * 150;

	public static int time(int code) {
		return code / XXX;
	}

	public static int x(int code) {
		int withoutTime = code % XXX;
		return withoutTime / 150 + 1;
	}

	public static int y(int code) {
		int withoutTime = code % XXX;
		return withoutTime % 150 + 1;
	}

	public static int code(int time, int x, int y) {
		return (y - 1) + 150 * (x - 1) + XXX * time;
	}

}
