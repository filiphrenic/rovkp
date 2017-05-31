package similarity;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.common.LongPair;

public class CollaborativeItemSimilarityFast extends DefaultItemSimilarity {

	final private Cache<LongPair, Double> similarityCache;

	public CollaborativeItemSimilarityFast(DataModel model) {
		super(model);
		similarityCache = new Cache<>(this::itemSimilarityCached);
	}

	@Override
	public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
		return similarityCache.get(new LongPair(itemID1, itemID2));
	}

	private double itemSimilarityCached(LongPair lp) throws TasteException {
		long itemID1 = lp.getFirst();
		long itemID2 = lp.getSecond();
		double n1 = norm(itemID1);
		double n2 = norm(itemID2);
		if (n1 == 0.0 || n2 == 0.0) {
			return 0.0;
		}
		double totalPref = 0.0;

		DataModel model = getDataModel();
		LongPrimitiveIterator it = model.getUserIDs();
		while (it.hasNext()) {
			long userID = it.nextLong();

			Float f = model.getPreferenceValue(userID, itemID1);
			if (f == null) {
				continue;
			}
			double pref = f;
			f = model.getPreferenceValue(userID, itemID2);
			if (f == null) {
				continue;
			}
			pref *= f;
			totalPref += pref;
		}

		return totalPref / n1 / n2;
	}


}
