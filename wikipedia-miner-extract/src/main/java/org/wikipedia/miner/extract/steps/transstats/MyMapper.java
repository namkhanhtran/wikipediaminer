package org.wikipedia.miner.extract.steps.transstats;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.wikipedia.miner.db.struct.DbTranslations;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.util.MarkupStripper;

/**
 * 
 * @author tuan
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, IntWritable, DbTranslations> {

	private static Logger logger = Logger.getLogger(MyMapper.class) ;

	private Language language ;
	private SiteInfo siteInfo ;

	private DumpPageParser pageParser ;
	private DumpLinkParser linkParser ;

	private String rootCategoryTitle;

	private MarkupStripper stripper = new MarkupStripper() ;

	private final IntWritable keyOut = new IntWritable();
	
	// TODO: multiple outputs here
	
	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		try {

			language = null ;
			siteInfo = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
					siteInfo = SiteInfo.load(new File(cf.toString())) ;
				}

				if (cf.getName().equals(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					language = Languages.load(new File(cf.toString())).get(conf.get(DumpExtractor.KEY_LANG_CODE)) ;
				}
			}

			if (siteInfo == null) 
				throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

			if (language == null) 
				throw new Exception("Could not locate '" + conf.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			pageParser = new DumpPageParser(language, siteInfo) ;
			linkParser = new DumpLinkParser(language, siteInfo) ;

			rootCategoryTitle = Util.normaliseTitle(language.getRootCategory()) ;
			
		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}
	}


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		DumpPage parsedPage = null ;

		//build up translations tree map
		TreeMap<String, String> translationsByLangCode = new TreeMap<String, String>() ;

		try {
			parsedPage = pageParser.parsePage(value.toString()) ;
		} catch (Exception e) {
			context.getCounter(TransAndStatsStep.PageCounter.unparseable).increment(1);
			logger.error("Could not parse dump page " , e) ;
		}

		if (parsedPage == null)
			return ;

		switch (parsedPage.getType()) {

		case article :
			context.getCounter(TransAndStatsStep.PageCounter.articleCount).increment(1);
			handleArticleOrCategory(parsedPage, translationsByLangCode, context);
			
			break ;
		case category :
			context.getCounter(TransAndStatsStep.PageCounter.categoryCount).increment(1);

			if (rootCategoryTitle.equals(parsedPage.getTitle())) {
				context.getCounter(TransAndStatsStep.PageCounter.rootCategoryCount).increment(1);
				context.getCounter(TransAndStatsStep.PageCounter.rootCategoryId).increment(parsedPage.getId());
			}
            handleArticleOrCategory(parsedPage, translationsByLangCode, context);
			
			break ;
		case disambiguation :
			context.getCounter(TransAndStatsStep.PageCounter.disambiguationCount).increment(1);

			break ;
		case redirect :
			if (parsedPage.getNamespace().getKey() == SiteInfo.MAIN_KEY)
				context.getCounter(TransAndStatsStep.PageCounter.articleRedirect).increment(1);

			if (parsedPage.getNamespace().getKey() == SiteInfo.CATEGORY_KEY)
				context.getCounter(TransAndStatsStep.PageCounter.categoryRedirect).increment(1);

			context.getCounter(TransAndStatsStep.PageCounter.redirectCount).increment(1);

			break ;
		default:
			//for all other page types (e.g. templates) do nothing
			return ;
		}
		
		// emit collected translations
		if (!translationsByLangCode.isEmpty()) {
			keyOut.set(parsedPage.getId());
			context.write(keyOut, new DbTranslations(translationsByLangCode));
		}
	}

	private PageKey buildKey(DumpPage parsedPage) {

		PageKey key = new PageKey();

		key.setNamespace(parsedPage.getNamespace().getKey());
		key.setTitle(parsedPage.getTitle());

		return key ;
	}

	private void handleArticleOrCategory(DumpPage parsedPage, TreeMap<String, String> translationsByLangCode, Context context) throws IOException, InterruptedException {


		PageKey key = buildKey(parsedPage) ;
		PageDetail page = buildBasePageDetails(parsedPage) ;

		handleLinks(key, page,  parsedPage.getMarkup(), translationsByLangCode, context) ;
	}

	private PageDetail buildBasePageDetails(DumpPage parsedPage) {


		PageDetail page = PageSummaryStep.buildEmptyPageDetail() ;

		page.setId(parsedPage.getId());

		if (parsedPage.getType().equals(PageType.disambiguation))
			page.setIsDisambiguation(true);

		//note: we don't set namespace or title, because these will be found in page keys (so it would be wasteful to repeat them)

		if (parsedPage.getTarget() != null)
			page.setRedirectsTo(new PageSummary(-1,parsedPage.getTarget(), parsedPage.getNamespace().getKey(), false));

		if (parsedPage.getLastEdited() != null)
			page.setLastEdited(parsedPage.getLastEdited().getTime());


		return page ;
	}


	public void handleLinks(PageKey key, PageDetail page, String markup, TreeMap<String,String> translationsByLangCode, Context context) throws IOException, InterruptedException {

		/*String strippedMarkup = null ;

		try {
			strippedMarkup = stripper.stripAllButInternalLinksAndEmphasis(markup, ' ') ;
		} catch (Exception e) {

			logger.warn("Could not process link markup for " + page.getId() + ":" + key.getTitle());
			return ;
		}

		Vector<int[]> linkRegions = stripper.gatherComplexRegions(strippedMarkup, "\\[\\[", "\\]\\]") ;*/
		Vector<int[]> linkRegions = stripper.gatherComplexRegions(markup, "\\[\\[", "\\]\\]") ;

		for(int[] linkRegion: linkRegions) {			
			//String linkMarkup = strippedMarkup.substring(linkRegion[0]+2, linkRegion[1]-2) ;
			String linkMarkup = markup.substring(linkRegion[0]+2, linkRegion[1]-2) ;

			DumpLink link = null ;
			try {
				link = linkParser.parseLink(linkMarkup, key.getTitle().toString()) ;
			} catch (Exception e) {
				logger.warn("Could not parse link markup '" + linkMarkup + "'") ;
			}

			if (link == null)
				continue ;

			if (link.getTargetLanguage() != null) {
				//logger.info("Language link: " + linkMarkup);

				//TODO: how do we get translations now?

				if (translationsByLangCode != null) {
					translationsByLangCode.put(link.getTargetLanguage(), link.getAnchor()) ;
				}
			}			
		}	
	}
}
