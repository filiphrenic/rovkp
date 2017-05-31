package similarity;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.common.LongPair;

import java.util.HashMap;
import java.util.Map;

public class CollaborativeItemSimilarity extends DefaultItemSimilarity {

	private final Map<LongPair, Double> matrix;

	public CollaborativeItemSimilarity(DataModel model) throws TasteException {
		super(model);
		matrix = new HashMap<>();

		// calculate matrix

		final Map<Long, Double> norms = new HashMap<>();
		LongPrimitiveIterator iterator = model.getUserIDs();

		while (iterator.hasNext()) {

			final long userID = iterator.nextLong();
			final Cache<Long, Float> cache =
					new Cache<>(itemID -> model.getPreferenceValue(userID, itemID));

			for (long itemID1 : model.getItemIDsFromUser(userID)) {
				double pref1 = cache.get(itemID1);
				norms.put(itemID1, norms.getOrDefault(itemID1, 0.0) + Math.pow(pref1, 2));

				for (long itemID2 : model.getItemIDsFromUser(userID)) {
					double pref = pref1 * cache.get(itemID2);
					LongPair lp = new LongPair(itemID1, itemID2);
					matrix.put(lp, matrix.getOrDefault(lp, 0.0) + pref);
				}
			}
		}

		matrix.replaceAll(
				(k, v) -> v == 0
						? 0
						: v / Math.sqrt(norms.get(k.getFirst()) * norms.get(k.getSecond()))
		);
	}

	@Override
	public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
		return matrix.getOrDefault(new LongPair(itemID1, itemID2), 0.0);
	}

}
