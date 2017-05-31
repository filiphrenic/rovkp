package second;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;

// prvi program
public class ItemBased extends Main {

	public static void main(String[] args) throws Exception {
		Main.run(args, ItemBased::new);
	}

	private ItemSimilarity similarity;

	private ItemBased(String dir) {
		super(dir);
		similarity = new FileItemSimilarity(new File(dir + "/items_similarity.csv"));
	}

	@Override
	public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		return new GenericItemBasedRecommender(dataModel, new CachingItemSimilarity(similarity, dataModel));
	}
}
