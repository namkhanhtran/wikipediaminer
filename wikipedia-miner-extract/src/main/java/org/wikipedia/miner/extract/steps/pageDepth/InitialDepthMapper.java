package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;

public class InitialDepthMapper extends Mapper<AvroKey<Integer>, AvroValue<PageDetail>, 
		AvroKey<Integer>, AvroValue<PageDepthSummary>> {

	private static Logger logger = Logger.getLogger(SubsequentDepthMapper.class) ;
	
	private String rootCategoryTitle ;
	
	
	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		try {

			Language language = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					language = Languages.load(new File(cf.toString())).get(conf.get(DumpExtractor.KEY_LANG_CODE)) ;
				}

			}

			if (language == null) 
				throw new Exception("Could not locate '" + conf.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			rootCategoryTitle = Util.normaliseTitle(language.getRootCategory()) ;

			
			
		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}
		
		logger.info(rootCategoryTitle) ;
	}
	
	
	@Override
	public void map(AvroKey<Integer> pageKey, AvroValue<PageDetail> pageValue, Context context) throws IOException, InterruptedException {
		
		if (rootCategoryTitle == null)
			throw new IOException("Mapper not configured with root category title") ;
		
		PageDetail page = pageValue.datum() ;
		
		if (!page.getNamespace().equals(SiteInfo.CATEGORY_KEY) && !page.getNamespace().equals(SiteInfo.MAIN_KEY)) {
			//this only effects articles and categories, just discard other page types
			return ;
		}
		
		if (page.getRedirectsTo() != null) {
			//this doesn't effect redirects, so just discard them
			return ;
		}
		
		PageDepthSummary depthSummary = new PageDepthSummary() ;
		depthSummary.setChildIds(new ArrayList<Integer>()) ;
		
		for (PageSummary childCat:page.getChildCategories()) 
			depthSummary.getChildIds().add(childCat.getId()) ;
		
		for (PageSummary childArt:page.getChildArticles())
			depthSummary.getChildIds().add(childArt.getId()) ;
		
		if (rootCategoryTitle.equals(page.getTitle().toString())) {
			depthSummary.setDepth(0) ;
			shareDepth(depthSummary, context) ;
		} 
		
		context.write(new AvroKey<Integer>(page.getId()), new AvroValue<PageDepthSummary>(depthSummary)) ;
	}
	
	public static void shareDepth(PageDepthSummary page, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		
		if (page.getDepth() == null)
			return ;
		
		if (page.getDepthForwarded())
			return ;
		
		//logger.info("sharing depths for " + page.getTitle() + ": " + page.getDepth());
		for (Integer childId:page.getChildIds()) {
			
			PageDepthSummary child = new PageDepthSummary() ;
			child.setDepth(page.getDepth() + 1);
			child.setDepthForwarded(false);
			child.setChildIds(new ArrayList<Integer>());
			
			context.write(new AvroKey<Integer>(childId), new AvroValue<PageDepthSummary>(child)) ;
		}
		
		page.setDepthForwarded(true);
	}	
}
