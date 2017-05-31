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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

public class Main {

	public static void main(String[] args) throws Exception {
//		first(args);
//		second(args);
	}

	private static void first(String[] args) throws IOException {
		PrintWriter pw = new PrintWriter(Files.newOutputStream(Paths.get("pollutionData-all.csv")));
		Files.list(Paths.get("files"))
				.flatMap(EFunction.wrap(Files::lines))
				.map(PollutionReading::create)
				.filter(Objects::nonNull)
				.sorted(Comparator.comparing(PollutionReading::getTimestamp))
				.map(PollutionReading::getReading)
				.forEach(pw::println);
	}

	private static void second(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Second task");
		try {
			conf.get("spark.master");
		} catch (NoSuchElementException ex) {
			conf.setMaster("local");
		}

		JavaRDD<DeathRecord> records = new JavaSparkContext(conf)
				.textFile("DeathRecords.csv")
				.map(DeathRecord::create)
				.filter(Objects::nonNull);

//		s1(records);
//		s2(records);
//		s3(records);
//		s4(records);
//		s5(records);
//		s6(records);
		s7(records);
	}

	private static void s1(JavaRDD<DeathRecord> records) {
		long deaths = records.filter(r -> !r.isMale())
				.filter(r -> r.getMonthOfDeath() == 6)
				.count();

		System.out.println(deaths); // 99929
	}

	private static void s2(JavaRDD<DeathRecord> records) {
		Integer dayOfWeek = records.filter(DeathRecord::isMale)
				.filter(r -> r.getAge() > 50)
				.mapToPair(r -> new Tuple2<>(r.getDayOfWeekOfDeath(), 1L))
				.reduceByKey((x, y) -> x + y)
				.max(COMP)
				._1;

		System.out.println(dayOfWeek); // 4 = Wednesday
	}

	private static void s3(JavaRDD<DeathRecord> records) {
		long autopsy = records.filter(DeathRecord::isAutopsy).count();
		System.out.println(autopsy); // 197572
	}

	private static JavaRDD<DeathRecord> filterMan4560(JavaRDD<DeathRecord> records) {
		return records.filter(DeathRecord::isMale)
				.filter(r -> r.getAge() > 45 && r.getAge() < 60);
	}

	private static JavaPairRDD<Integer, Long> s4(JavaRDD<DeathRecord> records) {
		JavaPairRDD<Integer, Long> byMonth = filterMan4560(records)
				.mapToPair(r -> new Tuple2<>(r.getMonthOfDeath(), 1L))
				.reduceByKey((x, y) -> x + y)
				.sortByKey();

		byMonth.collect().forEach(System.out::println);

		return byMonth;
	}

	private static void s5(JavaRDD<DeathRecord> records) {
		JavaPairRDD<Integer, Double> percentage = filterMan4560(records).filter(DeathRecord::isMarried)
				.mapToPair(r -> new Tuple2<>(r.getMonthOfDeath(), 1L))
				.reduceByKey((x, y) -> x + y)
				.sortByKey()
				.join(s4(records))
				.mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2));

		percentage.collect().forEach(System.out::println);
	}

	private static void s6(JavaRDD<DeathRecord> records) {
		long accident = records.filter(DeathRecord::isAccident).count();
		System.out.println(accident); // 131629
	}

	private static void s7(JavaRDD<DeathRecord> records){
		long ages = records.map(DeathRecord::getAge).distinct().count();
		System.out.println(ages); // 118
	}


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

	private static final Comparator<Tuple2<Integer, Long>> COMP = new MyComp<>();

	private static final class MyComp<T> implements Comparator<Tuple2<T, Long>>, Serializable {

		@Override
		public int compare(Tuple2<T, Long> o1, Tuple2<T, Long> o2) {
			return Long.compare(o1._2, o2._2);
		}
	}

}
