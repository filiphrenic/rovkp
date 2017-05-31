import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

public class Main {

	public static void main(String[] args) throws Exception {
//		first(args);
		second(args);
	}

	private static void first(String[] args) throws IOException {
		PrintWriter pw = new PrintWriter(Files.newOutputStream(Paths.get("sensorscope-monitor-all.csv")));
		Files.list(Paths.get("files"))
				.flatMap(EFunction.wrap(Files::lines))
				.map(SensorscopeReading::create)
				.filter(Objects::nonNull)
				.sorted(Comparator.comparing(SensorscopeReading::getTime))
				.map(SensorscopeReading::getReading)
				.forEach(pw::println);
		// 97 ulazna fajla
		// 4726495 linije
		// 457 MB
	}

	private static void second(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Second task");
		try {
			conf.get("spark.master");
		} catch (NoSuchElementException ex) {
			conf.setMaster("local");
		}

		JavaRDD<USBabyNameRecord> babies = new JavaSparkContext(conf)
				.textFile("StateNames.csv")
				.map(USBabyNameRecord::create)
				.filter(Objects::nonNull);

		s1(babies);
//		s2(babies);
//		s3(babies);
//		s4(babies);
//		s5(babies);
//		s6(babies);
//		s7(babies);
	}

	private static void s1(JavaRDD<USBabyNameRecord> babies) {
		// 1) find least popular female name
		String first = babies.filter(r -> !r.isMale())
				.mapToPair(r -> new Tuple2<>(r.getName(), r.getCount()))
				.reduceByKey((x, y) -> x + y)
				.min(COMP)
				._1;
		System.out.println(first);
	}

	private static void s2(JavaRDD<USBabyNameRecord> babies) {
		// 2) 10 most popular male names
		babies.filter(USBabyNameRecord::isMale)
				.mapToPair(r -> new Tuple2<>(r.getName(), r.getCount()))
				.reduceByKey((x, y) -> x + y)
				.top(10, COMP)
				.forEach(System.out::println);
	}

	private static void s3(JavaRDD<USBabyNameRecord> babies) {
		// 3) State with most children born in 1946
		String state = babies.filter(r -> r.getYear() == 1946)
				.mapToPair(r -> new Tuple2<>(r.getState(), r.getCount()))
				.reduceByKey((x, y) -> x + y)
				.max(COMP)
				._1;
		System.out.println(state);
	}

	private static JavaPairRDD<Integer, Long> s4(JavaRDD<USBabyNameRecord> babies) {
		// 4) Number of female children through years
		JavaPairRDD<Integer, Long> years = babies.filter(r -> !r.isMale())
				.mapToPair(r -> new Tuple2<>(r.getYear(), r.getCount()))
				.reduceByKey((x, y) -> x + y)
//				.mapToPair(Tuple2::swap)
				.sortByKey()
//				.mapToPair(Tuple2::swap)
				;

		years.collect().forEach(System.out::println);

		return years;
	}

	private static void s5(JavaRDD<USBabyNameRecord> babies) {
		// 5) Percentage of name "Mary"
		JavaPairRDD<Integer, Long> years = s4(babies);

		List<Tuple2<Integer, Double>> mary = babies.filter(r -> !r.isMale())
				.filter(r -> "Mary".equals(r.getName()))
				.mapToPair(r -> new Tuple2<>(r.getYear(), r.getCount()))
				.reduceByKey((x, y) -> x + y)
				.join(years)
				.mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2))
				.sortByKey()
				.collect();

		mary.forEach(System.out::println);
	}

	private static void s6(JavaRDD<USBabyNameRecord> babies) {
		// 6) Number of children born
		long childrenBorn = babies.map(USBabyNameRecord::getCount).reduce((x, y) -> x + y);
		System.out.println(childrenBorn); // 298883326
	}

	private static void s7(JavaRDD<USBabyNameRecord> babies) {
		// 6) Number of distinct names
		long distinctName = babies.map(USBabyNameRecord::getName).distinct().count();
		System.out.println(distinctName); // 30274
	}

	// HELPERS

	@FunctionalInterface
	public interface EFunction<T, R> extends Function<T, R> {

		@Override
		default R apply(T t) {
			try {
				return throwingApply(t);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		static <T, R> Function<T, R> wrap(EFunction<T, R> f) {
			return f;
		}

		R throwingApply(T t) throws Exception;
	}

	private static final Comparator<Tuple2<String, Long>> COMP = new MyComp<>();

	private static final class MyComp<T> implements Comparator<Tuple2<T, Long>>, Serializable {

		@Override
		public int compare(Tuple2<T, Long> o1, Tuple2<T, Long> o2) {
			return Long.compare(o1._2, o2._2);
		}
	}

}
