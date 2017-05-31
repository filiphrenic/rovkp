import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
class SensorStreamGenerator extends Thread {

	private static final int WAIT_PERIOD_IN_MILLISECONDS = 1;
	static final int PORT = 10002;

	private String inputFile;

	private SensorStreamGenerator(String inputFile) {
		this.inputFile = inputFile;
	}

	@Override
	public void run() {
		System.out.println("Waiting for client connection");

		try (ServerSocket serverSocket = new ServerSocket(PORT);
		     Socket clientSocket = serverSocket.accept()) {

			System.out.println("Connection successful");

			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);

			Files.lines(Paths.get(inputFile)).forEach(line -> {
				out.println(line);
				try {
					TimeUnit.MILLISECONDS.sleep(WAIT_PERIOD_IN_MILLISECONDS);
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
			});

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	static void start(String inputFile) throws IOException {
		new SensorStreamGenerator(inputFile).start();
	}
}