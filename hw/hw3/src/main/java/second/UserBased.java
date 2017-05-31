package second;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

// drugi program
public class UserBased extends Main {

	private static final boolean useLogLikelihood = false;

	public static void main(String[] args) throws Exception {
		Main.run(args, UserBased::new);
	}

	private UserBased(String dir) {
		super(dir);
	}

	@Override
	public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		UserSimilarity similarity =
//				= useLogLikelihood
//				? new LogLikelihoodSimilarity(dataModel)
//				:
		new PearsonCorrelationSimilarity(dataModel);
		similarity = new CachingUserSimilarity(similarity, dataModel);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(dataModel.getNumUsers(), 0.1, similarity, dataModel);
		return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
	}

	@Override
	void finish() throws TasteException {
		int users = 100;
		int rec = 10;
		Recommender r = buildRecommender(dataModel);
		LongPrimitiveIterator it = dataModel.getUserIDs();
		for (; it.hasNext() && users > 0; users--) {
			long id = it.nextLong();
			for (RecommendedItem ri : r.recommend(id, rec)) {
				// TODO to file
				System.out.printf("%d,%d,%f%n", id, ri.getItemID(), ri.getValue());
			}
		}
	}
}
