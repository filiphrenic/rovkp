package similarity;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HybridItemSimilarity extends DefaultItemSimilarity {

	private double[] weights;
	private List<ItemSimilarity> similarities;

	public static ItemSimilarity create(DataModel model, double[] weights, ItemSimilarity... similarities) throws TasteException {
		if (weights == null || similarities == null || weights.length != similarities.length) {
			throw new RuntimeException("Must provide same amount of weights as similarities");
		}

		List<ItemSimilarity> list = Arrays.stream(similarities)
				.map(s -> new NormalizedItemSimilarity(model, s))
				.collect(Collectors.toList());

		ItemSimilarity similarity = new HybridItemSimilarity(model, weights, list);

		return new CachingItemSimilarity(similarity, model);
	}

	private HybridItemSimilarity(DataModel model, double[] weights, List<ItemSimilarity> similarities) throws TasteException {
		super(model);
		this.weights = weights;
		this.similarities = similarities;
	}

	@Override
	public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
		double s = 0.0;
		for (int i = 0; i < weights.length; i++) {
			s += weights[i] * similarities.get(i).itemSimilarity(itemID1, itemID2);
		}
		return s;
	}

}
