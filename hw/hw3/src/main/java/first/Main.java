package first;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {

	private static final String ITEMS_PATH = "jester_items.dat";
	private static final String SIMILARITY_PATH = "items_similarity.csv";


	public static void main(String[] args) throws IOException, ParseException {
		if (args.length != 1) {
			System.err.println("Expecting path to directory with files");
			System.exit(1);
		}
		String dir = args[0];
		if (!Files.isDirectory(Paths.get(dir))) {
			System.err.println("Path is not directory");
			System.exit(2);
		}
		Path items = Paths.get(dir + "/" + ITEMS_PATH);
		if (!Files.isRegularFile(items)) {
			System.err.println("Items file not found");
			System.exit(3);
		}

		StandardAnalyzer analyzer = new StandardAnalyzer();
		Directory index = new RAMDirectory();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter writer = new IndexWriter(index, config);

		// 1. dio
		Map<Integer, String> jokes = getJokes(items);

		// 2. dio
		for (Map.Entry<Integer, String> e : jokes.entrySet()) {
			writer.addDocument(document(e));
		}
		writer.close();


		// 3. dio
		IndexReader reader = DirectoryReader.open(index);
		IndexSearcher searcher = new IndexSearcher(reader);
		float[][] matrix = calculateSimilarity(jokes, new QueryParser("text", analyzer), searcher);
		reader.close();

		// 4. dio
		matrix = normalize(matrix);

		// 5. dio
		int numLines = writeToFile(matrix, dir + "/" + SIMILARITY_PATH);

		// pitanja
		System.out.println("Zapisa ima: " + numLines);
		System.out.println(jokes.get(1));
		System.out.println(jokes.get(mostSimilarID(matrix, 1)));
	}

	private static int mostSimilarID(float[][] matrix, int ID) {
		int id = -1;
		float similar = 0;
		for (int i = 0; i < matrix[ID].length; i++) {
			if (i == ID) {
				continue;
			}
			if (matrix[ID][i] > similar) {
				similar = matrix[ID][i];
				id = i;
			}
		}
		return id;
	}

	// 5. dio
	private static int writeToFile(float[][] matrix, String filename) {
		int numLines = 0;
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename))) {
			for (int i = 0; i < matrix.length; i++) {
				for (int j = i + 1; j < matrix[i].length; j++) {
					if (matrix[i][j] > 0) {
						writer.write(String.format("%d,%d,%f%n", i, j, matrix[i][j]));
						numLines++;
					}
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return numLines;
	}

	// 4. dio
	private static float[][] normalize(float[][] matrix) {
		for (int i = 0; i < matrix.length; i++) {
			float maxi = 0;
			for (float x : matrix[i]) {
				maxi = Math.max(x, maxi);
			}
			if (maxi > 0) {
				for (int j = 0; j < matrix[i].length; j++) {
					matrix[i][j] /= maxi;
				}
			}
		}

		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j <= i; j++) {
				float x = (matrix[i][j] + matrix[j][i]) * .5f;
				matrix[i][j] = matrix[j][i] = x;
			}
		}

		return matrix;
	}

	// 3. dio
	private static float[][] calculateSimilarity(Map<Integer, String> jokes, QueryParser parser, IndexSearcher searcher)
			throws IOException, ParseException {


		int n = jokes.size();
		float[][] similarity = new float[n + 1][n + 1];

		for (Map.Entry<Integer, String> e : jokes.entrySet()) {
			int i = e.getKey();
			Query query = parser.parse(QueryParser.escape(e.getValue()));
			TopDocs docs = searcher.search(query, n);
			for (ScoreDoc hit : docs.scoreDocs) {
				int j = Integer.parseInt(searcher.doc(hit.doc).get("ID"));
				similarity[i][j] = hit.score;
			}
		}

		return similarity;
	}

	// 2. dio
	private static Document document(Map.Entry<Integer, String> entry) {
		Document doc = new Document();
		doc.add(new Field("ID", entry.getKey().toString(), ID_FIELD_TYPE));
		doc.add(new Field("text", entry.getValue(), TEXT_FIELD_TYPE));
		return doc;
	}

	private static final FieldType ID_FIELD_TYPE;
	private static final FieldType TEXT_FIELD_TYPE;

	static {
		ID_FIELD_TYPE = new FieldType();
		ID_FIELD_TYPE.setStored(true);
		ID_FIELD_TYPE.setTokenized(false);
		ID_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);

		TEXT_FIELD_TYPE = new FieldType();
		TEXT_FIELD_TYPE.setStored(false);
		TEXT_FIELD_TYPE.setTokenized(true);
		TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
	}

	// 1. dio
	private static Map<Integer, String> getJokes(Path path) {
		Map<Integer, String> jokes = new HashMap<>();

		try (BufferedReader reader = Files.newBufferedReader(path)) {
			StringBuilder builder = new StringBuilder();

			while (true) {
				builder.setLength(0);
				String idString = reader.readLine();
				if (idString == null) {
					break;
				}
				int id = Integer.parseInt(idString.substring(0, idString.indexOf(":")));

				String line;
				while ((line = reader.readLine()) != null) {
					line = line.trim();
					if (line.isEmpty()) {
						break;
					}
					builder.append(line);
				}

				String joke = StringEscapeUtils.unescapeXml(builder.toString().toLowerCase().replaceAll("<.*?>", ""));

				jokes.put(id, joke);
			}

		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		return jokes;
	}
}

// treci
/*
	mahout recommenditembased --similarityClassname SIMILARITY_PEARSON_CORRELATION --input dz3/jester_ratings.csv --usersFile dz3/users.txt --output dz3/result --numRecommendations 10
 */