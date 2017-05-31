package similarity;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

public class NormalizedItemSimilarity extends DefaultItemSimilarity {

	private ItemSimilarity similarity;

	NormalizedItemSimilarity(DataModel model, ItemSimilarity similarity) {
		super(model);
		try {
			this.similarity = new CachingItemSimilarity(similarity, model);
		} catch (TasteException ex) {
			this.similarity = similarity;
		}
	}

	@Override
	public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
		return similarity.itemSimilarity(itemID1, itemID2) / sum(itemID1);
	}

	@Override
	public double itemSimilarityNormal(long itemID1, long itemID2) throws TasteException {
		return similarity.itemSimilarity(itemID1, itemID2);
	}
}
