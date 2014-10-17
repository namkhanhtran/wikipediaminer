package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.util.MarkupStripper;


public class MyMapper extends Mapper<LongWritable, Text, AvroKey<CharSequence>, AvroValue<LabelOccurrences>> {

	private static Logger logger = Logger.getLogger(Mapper.class) ;

	private Language language ;
	private SiteInfo siteInfo ;

	private DumpPageParser pageParser ;
	private DumpLinkParser linkParser ;

	private MarkupStripper stripper = new MarkupStripper() ;
	private PageSentenceExtractor sentenceExtractor ;

	private int totalLabels ;
	private List<Path> labelPaths ;
	LabelCache labelCache ;

	private final AvroKey<CharSequence> keyOut = new AvroKey<CharSequence>();
	private final AvroValue<LabelOccurrences> valOut = new AvroValue<LabelOccurrences>(),
	
	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		try {

			language = null ;
			siteInfo = null ;

			labelPaths = new ArrayList<Path>() ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
					siteInfo = SiteInfo.load(new File(cf.toString())) ;
				} else if (cf.getName().equals(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					language = Languages.load(new File(cf.toString())).get(conf.get(DumpExtractor.KEY_LANG_CODE)) ;
				} else if (cf.getName().equals(new Path(conf.get(DumpExtractor.KEY_SENTENCE_MODEL)).getName())) {
					sentenceExtractor = new PageSentenceExtractor(cf) ;
				} else {
					//assume this contains the labels and senses
					labelPaths.add(cf) ;
				}
			}

			if (siteInfo == null) 
				throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

			if (language == null) 
				throw new Exception("Could not locate '" + conf.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			if (labelPaths.isEmpty())
				throw new Exception("Could not locate any label files in DistributedCache") ;


			try {
				pageParser = new DumpPageParser(language, siteInfo) ;
				linkParser = new DumpLinkParser(language, siteInfo) ;
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

			totalLabels = conf.getInt(LabelOccurrenceStep.KEY_TOTAL_LABELS, 0) ;

			if (totalLabels == 0)
				throw new Exception("Could not retrieve total number of labels") ;

		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}

		labelCache = LabelCache.get();		
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (!labelCache.isLoaded()) 
			labelCache.load(labelPaths, totalLabels, context);

		DumpPage parsedPage = null ;

		try {
			parsedPage = pageParser.parsePage(value.toString()) ;
		} catch (Exception e) {
			//reporter.incrCounter(PageType.unparseable, 1);
			logger.error("Could not parse dump page " , e) ;
		}

		if (parsedPage == null)
			return ;

		//only care about articles
		if (parsedPage.getNamespace().getKey() != SiteInfo.MAIN_KEY)
			return ;

		//dont care about redirects
		if (parsedPage.getTarget() != null)
			return ;

		Map<CharSequence,LabelOccurrences> labels = new HashMap<CharSequence,LabelOccurrences>() ;

		String markup = parsedPage.getMarkup() ;

		try {

			markup = stripper.stripAllButInternalLinksAndEmphasis(markup, null) ;
			//markup = stripper.stripEmphasis(markup, null) ;

		} catch (Exception e) {
			logger.error("Could not strip markup: " + markup);
			return ;
		}

		labels = handleLinks(parsedPage, markup, labels, context) ;

		markup = stripper.stripInternalLinks(markup, null) ;


		int lastSplit = 0 ;



		for (int split:sentenceExtractor.getSentenceSplits(markup)) {

			labels = handleSentence(markup.substring(lastSplit, split), labels, context) ;
			lastSplit = split ;
		}

		labels = handleSentence(markup.substring(lastSplit), labels, context) ;

		for (Map.Entry<CharSequence, LabelOccurrences> e:labels.entrySet()) {
			// context.write(new AvroKey<CharSequence>(e.getKey()), new AvroValue<LabelOccurrences>(e.getValue()));
			keyOut.datum(e.getKey());
			valOut.datum(e.getValue());
			context.write(keyOut, valOut);
		}
		logger.info(parsedPage.getTitle() + ": " + labels.size() + " labels");

	}


	public Map<CharSequence,LabelOccurrences> handleLinks(DumpPage page, String markup, Map<CharSequence,LabelOccurrences> labels, Context context)  {

		//logger.info("markup: " + markup);

		Vector<int[]> linkRegions = stripper.gatherComplexRegions(markup, "\\[\\[", "\\]\\]") ;

		for(int[] linkRegion: linkRegions) {
			context.progress(); 

			String linkMarkup = markup.substring(linkRegion[0]+2, linkRegion[1]-2) ;

			DumpLink link = null ;
			try {
				link = linkParser.parseLink(linkMarkup, page.getTitle()) ;
			} catch (Exception e) {
				logger.warn("Could not parse link markup '" + linkMarkup + "'") ;
			}

			if (link == null)
				continue ;

			if (link.getTargetLanguage() != null) 
				continue ;

			if (link.getTargetNamespace().getKey() != SiteInfo.MAIN_KEY)
				continue ;

			LabelOccurrences lo = labels.get(link.getAnchor()) ;

			if (lo == null)
				lo = new LabelOccurrences(0,0,0,0) ;

			lo.setLinkDocCount(1);
			lo.setLinkOccCount(lo.getLinkOccCount() + 1);

			labels.put(link.getAnchor(), lo) ;
		}

		return labels ;
	}

	public Map<CharSequence,LabelOccurrences> handleSentence(String sentence, Map<CharSequence,LabelOccurrences> labels, Context context) {

		Tokenizer tokenizer = SimpleTokenizer.INSTANCE ;

		Span[] spans = tokenizer.tokenizePos(sentence) ;

		for (int startIndex=0 ; startIndex<spans.length ; startIndex++) {
			context.progress(); 

			for (int endIndex=startIndex ; endIndex < startIndex + labelCache.getMaxSensibleLabelLength() && endIndex < spans.length ; endIndex++) {

				CharSequence label = sentence.substring(spans[startIndex].getStart(), spans[endIndex].getEnd()) ;

				//logger.info(" - " + label);

				if (!labelCache.mightContain(label))
					continue ;

				LabelOccurrences lo = labels.get(label) ;

				if (lo == null)
					lo = new LabelOccurrences(0,0,0,0) ;

				lo.setTextDocCount(1);
				lo.setTextOccCount(lo.getLinkOccCount() + 1);

				labels.put(label, lo) ;
			}
		}

		return labels ;
	}
}