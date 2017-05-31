package similarity;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.similarity.AbstractItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public abstract class DefaultItemSimilarity extends AbstractItemSimilarity {

	final private Map<Long, Double> sums;
	final private Map<Long, Double> norms;

	DefaultItemSimilarity(DataModel dataModel) {
		super(dataModel);
		norms = new HashMap<>();
		sums = new HashMap<>();
	}

	@Override
	public double[] itemSimilarities(long id1, long[] ids) throws TasteException {
		double[] similarities = new double[ids.length];
		for (int i = 0; i < ids.length; i++) {
			similarities[i] = itemSimilarity(id1, ids[i]);
		}
		return similarities;
	}

	@Override
	public void refresh(Collection<Refreshable> collection) {
		super.refresh(collection);
	}

	double norm(long id) throws TasteException {
		Double norm = norms.get(id);
		if (norm != null) {
			return norm;
		}
		norm = 0.0;

		DataModel model = getDataModel();
		LongPrimitiveIterator iterator = model.getUserIDs();

		while (iterator.hasNext()) {
			long id2 = iterator.nextLong();
			Float pref = model.getPreferenceValue(id2, id);
			norm += pref == null ? 0 : pref * pref;
		}

		norm = Math.sqrt(norm);
		norms.put(id, norm);
		return norm;
	}

	double sum(long id) throws TasteException {
		Double sum = sums.get(id);
		if (sum != null) {
			return sum;
		}

		sum = 0.0;
		LongPrimitiveIterator it = getDataModel().getItemIDs();
		while (it.hasNext()) {
			double is = itemSimilarityNormal(id, it.nextLong());
			if (!Double.isNaN(is)) {
				sum += is;
			}
		}
		sums.put(id, sum);

		return sum;
	}

	double itemSimilarityNormal(long itemID1, long itemID2) throws TasteException {
		return itemSimilarity(itemID1, itemID2);
	}
}
