package second;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.util.function.Function;

abstract class Main implements RecommenderBuilder {

	private static final double TRAINING_PERCENTAGE = 0.300000000000000000001;
	private static final double EVALUATION_PERCENTAGE = 0.699999999999999999999;

	static void run(String[] args, Function<String, Main> create) throws Exception {
		if (args.length != 1) {
			System.err.println("Expecting one argument: path to directory");
			System.exit(1);
		}
		Main g = create.apply(args[0]);
		g.recommend(220, 10);
		g.evaluate();
		g.finish();
	}

	DataModel dataModel;

	Main(String dir) {
		try {
			dataModel = new FileDataModel(new File(dir + "/jester_ratings.dat"), "\\s+");
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
	}

	private void recommend(long id, int n) throws Exception {
		Recommender recommender = buildRecommender(dataModel);
		System.out.printf("Top %d recommendations for user ID = %d%n", n, id);
		recommender.recommend(id, n)
				.forEach(r -> System.out.printf("\t[%3d] => %.3f%n", r.getItemID(), r.getValue()));
	}

	private void evaluate() throws Exception {
		RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
		double score = recEvaluator.evaluate(
				this,
				null,
				dataModel,
				TRAINING_PERCENTAGE,
				EVALUATION_PERCENTAGE
		);
		System.out.println("Recommender score => " + score);

		RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = statsEvaluator.evaluate(
				this, null, dataModel, null, 2,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0
		);
		System.out.printf("Precision => %.4f%n", stats.getPrecision());
		System.out.printf("Recall    => %.4f%n", stats.getRecall());
	}

	void finish() throws TasteException {
	}

}
