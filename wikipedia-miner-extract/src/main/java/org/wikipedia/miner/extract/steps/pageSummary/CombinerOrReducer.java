package org.wikipedia.miner.extract.steps.pageSummary;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.procedure.TIntProcedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep.Unforwarded;


public abstract class CombinerOrReducer extends Reducer<
		AvroKey<PageKey>, AvroValue<PageDetail>, AvroKey<PageKey>, AvroValue<PageDetail>> {

	private static Logger logger = Logger.getLogger(CombinerOrReducer.class) ;

	public abstract boolean isReducer() ;

	private CharSequence[] debugTitles = {"Atheist","Atheism","Atheists","Athiest","People by religion"} ;

	@Override
	public void reduce(AvroKey<PageKey> key, Iterable<AvroValue<PageDetail>> pagePartials, Context context) throws IOException, InterruptedException {

		Integer id = null;
		//Integer namespace = key.getNamespace() ;
		CharSequence title = key.datum().getTitle() ;
		Long lastEdited = null ;
		boolean isDisambiguation = false ;
		
		TIntArrayList sentenceSplits = new TIntArrayList() ;

		SortedMap<Integer,PageSummary> redirects = new TreeMap<Integer, PageSummary>() ;
		PageSummary redirectsTo = null ;

		SortedMap<Integer,LinkSummary> linksIn = new TreeMap<Integer, LinkSummary>() ;
		SortedMap<Integer,LinkSummary> linksOut = new TreeMap<Integer, LinkSummary>() ;

		SortedMap<Integer,PageSummary> parentCategories = new TreeMap<Integer, PageSummary>() ;
		SortedMap<Integer,PageSummary> childCategories = new TreeMap<Integer, PageSummary>() ;
		SortedMap<Integer,PageSummary> childArticles = new TreeMap<Integer, PageSummary>() ;

		SortedMap<CharSequence,LabelSummary> labels = new TreeMap<CharSequence,LabelSummary>() ;

		boolean debug = false ;
		for(CharSequence debugTitle:debugTitles) {
			if (title.equals(debugTitle))
				debug = true ;
		}

		if (debug)
			logger.info("Processing " + key.toString()) ;

		for (AvroValue<PageDetail> pagePartialProxy: pagePartials) {
			PageDetail pagePartial = pagePartialProxy.datum();

			if (debug)
				logger.info("partial: " + pagePartial.toString());

			if (pagePartial.getId() != Integer.MIN_VALUE)
				id = pagePartial.getId() ;

			if (pagePartial.getLastEdited() != null)
				lastEdited = pagePartial.getLastEdited() ;
			
			if (pagePartial.getIsDisambiguation())
				isDisambiguation = true ;

			if (pagePartial.getRedirectsTo() != null) {

				if (debug)
					logger.info(" -" + pagePartial.getRedirectsTo() + " vs " + redirectsTo) ;

				if (redirectsTo == null || redirectsTo.getId() < 0) {
					//always clobber a redirectTo that hasn't been resolved to an id yet

					redirectsTo = PageSummary.newBuilder(pagePartial.getRedirectsTo()).build() ;
				}else {

					if (pagePartial.getRedirectsTo().getForwarded())
						redirectsTo.setForwarded(true);	
				}

				if (debug)
					logger.info(" - " + redirectsTo) ;
			}
			
			//we cant to do a straight copy, because avro seems to reuse these instances.
			sentenceSplits.addAll(pagePartial.getSentenceSplits()) ;
				
			redirects = addToPageMap(pagePartial.getRedirects(), redirects) ;

			linksIn = addToLinkMap(pagePartial.getLinksIn(), linksIn) ;
			linksOut = addToLinkMap(pagePartial.getLinksOut(), linksOut) ;

			parentCategories = addToPageMap(pagePartial.getParentCategories(), parentCategories) ;
			childCategories = addToPageMap(pagePartial.getChildCategories(), childCategories) ;
			childArticles = addToPageMap(pagePartial.getChildArticles(), childArticles) ;

			for (Map.Entry<CharSequence, LabelSummary> e:pagePartial.getLabels().entrySet()) {
				
				CharSequence label = e.getKey() ;
				
				LabelSummary labelStats = labels.get(label) ;
				
				if (labelStats == null) 
					labelStats = new LabelSummary(0,0) ;
				
				labelStats.setDocCount(labelStats.getDocCount() + e.getValue().getDocCount());
				labelStats.setOccCount(labelStats.getOccCount() + e.getValue().getOccCount());

				labels.put(label, labelStats) ;
			}

		}

		if (id == null && isReducer()) {

			//if we don't know the id of the page by this point, then it must be the 
			//result of an unresolvable redirect or link (so forget it)
			//logger.warn("Orphaned page title: " + key.getTitle() + " in ns " + key.getNamespace()) ;

			return ;
		}


		if (debug) {

			for (Integer rId:redirects.keySet()) 
				logger.info(" - " + rId+ ":" + redirects.get(rId)) ;

		}

		PageDetail combinedPage = PageSummaryStep.buildEmptyPageDetail() ;
		combinedPage.setId(id)	;
		//combinedPage.setTitle(title);
		//combinedPage.setNamespace(namespace);
		combinedPage.setIsDisambiguation(isDisambiguation);
		combinedPage.setLastEdited(lastEdited) ;
		combinedPage.setSentenceSplits(sentenceSplits);

		combinedPage.setRedirectsTo(redirectsTo);

		boolean isRedirect = redirectsTo != null ;

		//redirects always need forwarding
		combinedPage.setRedirects(convertPagesToList(redirects, true));

		//links in always need to be backtracked (or forwarded by redirects) 
		combinedPage.setLinksIn(convertLinksToList(linksIn, true));

		//links out only need to be forwarded by redirects
		combinedPage.setLinksOut(convertLinksToList(linksOut, isRedirect));

		//parent categories only need to be forwarded by redirects 
		combinedPage.setParentCategories(convertPagesToList(parentCategories, isRedirect));

		//children of both types always need to be backtracked to parent (or forwarded by redirect)
		combinedPage.setChildCategories(convertPagesToList(childCategories, true));
		combinedPage.setChildArticles(convertPagesToList(childArticles, true));

		combinedPage.setLabels(labels);
		

		//count stuff that needs to be forwarded, so we know wheither another iteration is needed

		if (isReducer()) {

			countUnforwardedPages(Unforwarded.redirect, combinedPage.getRedirects(), context) ;

			if (redirectsTo != null && redirectsTo.getId() >= 0 && !redirectsTo.getForwarded())
				context.getCounter(Unforwarded.redirect).increment(1);

			countUnforwardedLinks(Unforwarded.linkIn, combinedPage.getLinksIn(), context) ;
			countUnforwardedLinks(Unforwarded.linkOut, combinedPage.getLinksOut(), context) ;

			countUnforwardedPages(Unforwarded.parentCategory, combinedPage.getParentCategories(), context) ;
			countUnforwardedPages(Unforwarded.childCategory, combinedPage.getChildCategories(), context) ;
			countUnforwardedPages(Unforwarded.childArticle, combinedPage.getChildArticles(), context) ;

		}

		if (debug)
			logger.info("combined: " + combinedPage.toString());

		context.write(key, new AvroValue<PageDetail>(combinedPage));
	}

	private SortedMap<Integer,PageSummary> addToPageMap(List<PageSummary> pages, SortedMap<Integer,PageSummary> pageMap) {

		if (pages == null || pages.isEmpty())
			return pageMap ;

		for (PageSummary link:pages) {

			//only overwrite if previous entry has not been forwarded
			PageSummary existingPage = pageMap.get(link.getId()) ;
			if (existingPage == null) {

				//the clone is needed because avro seems to reuse these instances.
				//if we don't clone it, it will get overwritten later
				pageMap.put(link.getId(), PageSummaryStep.clone(link)) ;

			} else {

				if (link.getForwarded())
					existingPage.setForwarded(true) ;

				//linkMap.put(existingLink.getId(), existingLink) ;
			}
		}

		return pageMap ;
	}


	private SortedMap<Integer,LinkSummary> addToLinkMap(List<LinkSummary> links, SortedMap<Integer,LinkSummary> linkMap) {

		if (links == null || links.isEmpty())
			return linkMap ;

		for (LinkSummary link:links) {

			//only overwrite if previous entry has not been forwarded
			final LinkSummary existingLink = linkMap.get(link.getId()) ;
			if (existingLink == null) {

				//the clone is needed because avro seems to reuse these instances.
				//if we don't clone it, it will get overwritten later
				linkMap.put(link.getId(), PageSummaryStep.clone(link)) ;

			} else {

				//merge lists of sentence indexes
				link.getSentenceIndexes().forEach(new TIntProcedure() {
					public boolean execute(int sentenceIndex) {
						int pos = existingLink.getSentenceIndexes().binarySearch(sentenceIndex) ;
						if (pos<0) 
							existingLink.getSentenceIndexes().insert((-pos) - 1, sentenceIndex) ;
						return true;
					}
				});

				//overwrite forwarded flag
				if (link.getForwarded())
					existingLink.setForwarded(true) ;

			}
		}

		return linkMap ;
	}

	private List<PageSummary> convertPagesToList(SortedMap<Integer,PageSummary> pageMap,  boolean requiresForwarding) {

		List<PageSummary> pages = new ArrayList<PageSummary>() ;

		for (PageSummary page:pageMap.values()) {
			if (!requiresForwarding) 
				page.setForwarded(true) ;

			pages.add(page) ;
		}

		return pages ;
	}
	
	private List<LinkSummary> convertLinksToList(SortedMap<Integer,LinkSummary> linkMap,  boolean requiresForwarding) {

		List<LinkSummary> links = new ArrayList<LinkSummary>() ;

		for (LinkSummary link:linkMap.values()) {
			if (!requiresForwarding) 
				link.setForwarded(true) ;

			links.add(link) ;
		}

		return links ;
	}

	private void countUnforwardedPages(Unforwarded counter, List<PageSummary> pages, Context context) {

		for (PageSummary page:pages) 				
			if (!page.getForwarded()) 
				context.getCounter(counter).increment(1);
	}
	
	private void countUnforwardedLinks(Unforwarded counter, List<LinkSummary> links, Context context) {

		for (LinkSummary link:links) 				
			if (!link.getForwarded()) 
				context.getCounter(counter).increment(1);
	}


	public static class MyCombiner extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return false;
		}

	}

	public static class MyReducer extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return true;
		}

	}
}