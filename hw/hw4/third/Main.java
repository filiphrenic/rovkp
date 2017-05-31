import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Expecting 2 args");
			System.exit(1);
		}

		SensorStreamGenerator.start(args[0]);
		SparkConf conf = new SparkConf().setAppName("Third task");
		try {
			conf.get("spark.master");
		} catch (NoSuchElementException ex) {
			conf.setMaster("local[2]");
		}

		JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));

		context.socketTextStream("localhost", SensorStreamGenerator.PORT)
				.map(SensorscopeReading::create)
				.filter(Objects::nonNull)
				.mapToPair(r -> new Tuple2<>(r.getStationID(), r.getSolarPanelCurrent()))
				.reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10))
				.dstream()
				.saveAsTextFiles(args[1], "");

		context.start();
		context.awaitTermination();

		// novi dir nastaje svakih 10 sec
		// svakih 5 sec se pokrece izracun
		// moze, zbog prozora

	}

}
