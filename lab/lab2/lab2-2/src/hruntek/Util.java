package hruntek;

public class Util {

	private static final int XXX = 150 * 150;

	static int time(int code) {
		return code / XXX;
	}

	static int x(int code) {
		int withoutTime = code % XXX;
		return withoutTime / 150 + 1;
	}

	static int y(int code) {
		int withoutTime = code % XXX;
		return withoutTime % 150 + 1;
	}

	static int code(int time, int x, int y) {
		return (y - 1) + 150 * (x - 1) + XXX * time;
	}

}
