/**
 * 
 */
package org.wikipedia.miner.training;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.wikipedia.miner.annotation.Disambiguator;
import org.wikipedia.miner.annotation.Topic;
import org.wikipedia.miner.annotation.TopicDetector;
import org.wikipedia.miner.annotation.preprocessing.DocumentPreprocessor;
import org.wikipedia.miner.annotation.preprocessing.PreprocessedDocument;
import org.wikipedia.miner.annotation.preprocessing.WikiPreprocessor;
import org.wikipedia.miner.annotation.tagging.DocumentTagger;
import org.wikipedia.miner.annotation.tagging.WikiTagger;
import org.wikipedia.miner.annotation.weighting.LinkDetector;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;

/**
 * @author ntran
 * 
 */
public class SnippetAnnotator {

	private DocumentPreprocessor preprocessor;

	private Disambiguator disambiguator;

	private TopicDetector topicDetector;

	private LinkDetector linkDetector;

	private DocumentTagger tagger;

	public DecimalFormat df = new DecimalFormat("#0%");

	public DateFormat dateFormat = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss zzz yyyy");

	/**
	 * Default Construction
	 * 
	 * @param wikipedia
	 * @throws Exception
	 */
	public SnippetAnnotator(Wikipedia wikipedia) throws Exception {
		preprocessor = new WikiPreprocessor(wikipedia);
		disambiguator = new Disambiguator(wikipedia);
		topicDetector = new TopicDetector(wikipedia, disambiguator);
		linkDetector = new LinkDetector(wikipedia);
		tagger = new WikiTagger();
	}

	private static final float THRESHOLD = 0.5f;

	/**
	 * Annotating document
	 * 
	 * @param originalMarkup
	 * @return
	 * @throws Exception
	 */
	public List<Topic> annotate(String originalMarkup) throws Exception {
		return annotate(originalMarkup, THRESHOLD);
	}

	/**
	 * Annotating document
	 * 
	 * @param originalMarkup
	 * @param threshold
	 *            - keep only topics with high probabilities
	 * @return
	 * @throws Exception
	 */
	public List<Topic> annotate(String originalMarkup, double threshold)
			throws Exception {
		PreprocessedDocument doc = preprocessor.preprocess(originalMarkup);

		Collection<Topic> allTopics = topicDetector.getTopics(doc, null);

		ArrayList<Topic> bestTopics = linkDetector.getBestTopics(allTopics,
				threshold);

		return bestTopics;

	}

	/**
	 * 
	 * @param inputFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	public HashMap<String, Integer> annotate(File inputFile) throws IOException {
		/*
		 * BufferedReader reader = new BufferedReader(new
		 * FileReader(inputFile)); String line = null; while ((line =
		 * reader.readLine()) != null) { String[] token = line.split("\t");
		 * String content = token[1]; Date date = dateFormat.parse(token[0]);
		 * System.out.println(content + "\n" + date.toString()); }
		 * reader.close();
		 */
		HashMap<String, Integer> tweetTopic = new HashMap<String, Integer>();

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line = null;
		int debugLine = 0;
		while ((line = reader.readLine()) != null) {
			if (line.length() == 0) {
				continue;
			}
			debugLine += 1;
			if (debugLine % 100 == 0) {
				System.out.println("Checking point " + debugLine);
			}
			String[] token = line.split("\t");
			String raw_content = token[4];

			JSONParser parser = new JSONParser();
			try {
				Object obj = parser.parse(raw_content);

				JSONObject jsonObject = (JSONObject) obj;

				String dateString = (String) jsonObject.get("created_at");

				// not yet used
				Date date = dateFormat.parse(dateString);

				String tweetText = (String) jsonObject.get("text");

				tweetText = cleanTweetText(tweetText);

				List<Topic> topicList = annotate(tweetText);

				for (Topic topic : topicList) {
					int topicId = topic.getId();
					String topicTitle = topic.getTitle();
					int weight = 1;
					if (tweetTopic.containsKey(topicTitle)) {
						weight += tweetTopic.get(topicTitle);
					}

					tweetTopic.put(topicTitle, weight);
				}
			} catch (ParseException e) {
				e.printStackTrace();
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		reader.close();
		return tweetTopic;
	}

	public void annotateMultipleFile(File directory) throws IOException {
		for (File file : directory.listFiles()) {
			if (!file.isFile() || !file.getName().endsWith(".tsv")) {
				continue;
			}
			System.out.println("Processing file : " + file.getName());
			Map<String, Integer> tweetTopic = annotate(file);
			tweetTopic = sortByComparator(tweetTopic);
			
			String filename = file.getName() + ".topic.20140503";
			FileWriter writer = new FileWriter(new File(directory, filename));
			for (Map.Entry<String, Integer> entry : tweetTopic.entrySet()) {
				writer.write(entry.getKey() + "\t" + entry.getValue());
				writer.write("\n");
			}
			writer.close();
		}
	}

	private static Map<String, Integer> sortByComparator(
			Map<String, Integer> unsortMap) {

		// Convert Map to List
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(
				unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it
				.hasNext();) {
			Map.Entry<String, Integer> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	/**
	 * Remove url, parse @ and #...
	 * 
	 * @param tweetText
	 * @return
	 */
	public String cleanTweetText(String tweetText) {
		// TODO: next step
		return tweetText;
	}

	/**
	 * @return the preprocessor
	 */
	public DocumentPreprocessor getPreprocessor() {
		return preprocessor;
	}

	/**
	 * @param preprocessor
	 *            the preprocessor to set
	 */
	public void setPreprocessor(DocumentPreprocessor preprocessor) {
		this.preprocessor = preprocessor;
	}

	/**
	 * @return the disambiguator
	 */
	public Disambiguator getDisambiguator() {
		return disambiguator;
	}

	/**
	 * @param disambiguator
	 *            the disambiguator to set
	 */
	public void setDisambiguator(Disambiguator disambiguator) {
		this.disambiguator = disambiguator;
	}

	/**
	 * @return the topicDetector
	 */
	public TopicDetector getTopicDetector() {
		return topicDetector;
	}

	/**
	 * @param topicDetector
	 *            the topicDetector to set
	 */
	public void setTopicDetector(TopicDetector topicDetector) {
		this.topicDetector = topicDetector;
	}

	/**
	 * @return the linkDetector
	 */
	public LinkDetector getLinkDetector() {
		return linkDetector;
	}

	/**
	 * @param linkDetector
	 *            the linkDetector to set
	 */
	public void setLinkDetector(LinkDetector linkDetector) {
		this.linkDetector = linkDetector;
	}

	/**
	 * @return the tagger
	 */
	public DocumentTagger getTagger() {
		return tagger;
	}

	/**
	 * @param tagger
	 *            the tagger to set
	 */
	public void setTagger(DocumentTagger tagger) {
		this.tagger = tagger;
	}

	public static void main(String[] args) throws Exception {
		WikipediaConfiguration conf = new WikipediaConfiguration(new File(
				args[0]));

		Wikipedia wikipedia = new Wikipedia(conf, false);
		SnippetAnnotator annotator = new SnippetAnnotator(wikipedia);

		annotator.annotateMultipleFile(new File(args[1]));
//		HashMap<String, Integer> tweetTopic = annotator.annotate(new File(
//				args[1]));
//		for (Map.Entry<String, Integer> topic : tweetTopic.entrySet()) {
//			System.out.println(topic.getKey() + ":\t" + topic.getValue());
//		}
	}
}
