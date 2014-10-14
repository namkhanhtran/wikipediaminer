/**
 * 
 */
package org.wikipedia.miner.training;

import java.io.File;

import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;

/**
 * @author ntran
 *
 */
public class SnippetAnnotatorTest {

	
	public static void main(String[] args) throws Exception {
		WikipediaConfiguration conf = new WikipediaConfiguration(new File(args[0]));
		
		Wikipedia wikipedia = new Wikipedia(conf, false);
		SnippetAnnotator annotator = new SnippetAnnotator(wikipedia);
		
		annotator.annotate(new File(args[1]));
	}
}
