package org.wikipedia.miner.extract.steps.pageSummary;

import gnu.trove.list.array.TIntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.steps.IterativeStep;
import org.wikipedia.miner.extract.steps.pageSummary.CombinerOrReducer.MyCombiner;
import org.wikipedia.miner.extract.steps.pageSummary.CombinerOrReducer.MyReducer;
import org.wikipedia.miner.extract.util.UncompletedStepException;
import org.wikipedia.miner.extract.util.XmlInputFormat;


/**
 * @author dmilne
 *
 * This step produces PageDetail structures. 
 * It needs to be run multiple times for the PageDetail structures to be completed (they get built gradually). 
 * 
 * Completion is indicated when all Unforwarded counters reach 0. 
 * 
 * The number of iterations needed is bounded by the longest chain of redirects
 *  (i.e. a redirect pointing to a redirect pointing to a redirect pointing to...)
 * 
 * The first iteration reads directly from the xml dump. 
 * Subsequent iterations read from the results of the previous iteration.
 * 
 * The page summaries will be missing namespace and title fields, because they are found in the page keys (so repeating them would be wasteful)
 *
 */
public class PageSummaryStep extends IterativeStep {

	private static Logger logger = Logger.getLogger(PageSummaryStep.class) ;

	public enum SummaryPageType {article, category, disambiguation, articleRedirect, categoryRedirect, unparseable} ; 
	public enum Unforwarded {redirect,linkIn,linkOut,parentCategory,childCategory,childArticle} ; 


	private Map<Unforwarded,Long> unforwardedCounts ;



	public PageSummaryStep(Path workingDir, int iteration) throws IOException {
		super(workingDir, iteration);


	}


	public boolean furtherIterationsRequired() {

		for (Long count:unforwardedCounts.values()) {
			if (count > 0)
				return true ;
		}

		return false ;

	}

	public static PageSummary clone(PageSummary summary) {

		return PageSummary.newBuilder(summary).build() ;
	}

	public static LinkSummary clone(LinkSummary summary) {

		return LinkSummary.newBuilder(summary).build() ;
	}

	public static PageDetail clone(PageDetail pageDetail) {

		return PageDetail.newBuilder(pageDetail).build() ;
	}

	public static PageDetail buildEmptyPageDetail() {

		PageDetail p = new PageDetail() ;
		p.setIsDisambiguation(false);
		p.setSentenceSplits(new TIntArrayList());
		p.setRedirects(new ArrayList<PageSummary>()) ;
		p.setLinksIn(new ArrayList<LinkSummary>());
		p.setLinksOut(new ArrayList<LinkSummary>());
		p.setParentCategories(new ArrayList<PageSummary>());
		p.setChildCategories(new ArrayList<PageSummary>());
		p.setChildArticles(new ArrayList<PageSummary>());
		p.setLabels(new HashMap<CharSequence,LabelSummary>()) ;

		return p ;
	}


	@Override
	public int run(String[] args) throws UncompletedStepException, IOException, ClassNotFoundException, InterruptedException {
		try {

			logger.info("Starting page summary step (iteration " + getIteration() + ")");

			if (isFinished()) {
				logger.info(" - already completed");
				loadUnforwardedCounts() ;

				return 0 ;
			} else
				reset() ;

			Job job = Job.getInstance(getConf());
			job.setJarByClass(PageSummaryStep.class);
			Configuration conf = job.getConfiguration();

			DumpExtractor.configureJob(job, args) ;

			job.setJobName("WM: page summary (" + getIteration() + ")");

			if (getIteration() == 0) {

				job.setMapperClass(InitialMapper.class);
				
				/*job.setOutputKeyClass(AvroKey.class);
				job.setOutputValueClass(AvroValue.class);*/

				job.setInputFormatClass(XmlInputFormat.class);
				job.getConfiguration().set(XmlInputFormat.START_TAG_KEY, "<page>") ;
				job.getConfiguration().set(XmlInputFormat.END_TAG_KEY, "</page>") ;

				FileInputFormat.setInputPaths(job, conf.get(DumpExtractor.KEY_INPUT_FILE));
				DistributedCache.addCacheFile(new Path(job.getConfiguration()
						.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);

			} else {

				job.setMapperClass(SubsequentMapper.class);
				AvroJob.setInputKeySchema(job, PageKey.getClassSchema());
				AvroJob.setInputValueSchema(job, PageDetail.getClassSchema());

				FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + "pageSummary_" + (getIteration()-1));

			}

			DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
			DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);

			job.setCombinerClass(MyCombiner.class) ;
			job.setReducerClass(MyReducer.class);
			
			AvroJob.setMapOutputKeySchema(job, PageKey.getClassSchema());
			AvroJob.setMapOutputValueSchema(job, PageDetail.getClassSchema());
			
			AvroJob.setOutputKeySchema(job, PageKey.getClassSchema());
			AvroJob.setOutputValueSchema(job, PageDetail.getClassSchema());

			FileOutputFormat.setOutputPath(job, getDir());

			logger.info("Finished setting up..");

			try {
				job.waitForCompletion(true);
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (job.isSuccessful()) {	
				finish(job) ;
				return 0 ;
			}

			// throw new UncompletedStepException() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}


	@Override
	public String getDirName(int iteration) {

		return "pageSummary_" + iteration ;
	}

	private Path getUnforwardedCountsPath() {
		return new Path(getDir() + Path.SEPARATOR + "unforwarded") ;
	}

	private void saveUnforwardedCounts() throws IOException {
		FSDataOutputStream out = getHdfs().create(getUnforwardedCountsPath());

		for (Unforwarded u:Unforwarded.values()) {

			out.writeUTF(u.name()) ;

			Long count = unforwardedCounts.get(u) ;
			if (count != null)
				out.writeLong(count) ;
			else
				out.writeLong(0L) ;
		}

		out.close();
	}

	private void loadUnforwardedCounts() throws IOException {

		unforwardedCounts = new HashMap<Unforwarded,Long>() ;


		FSDataInputStream in = getHdfs().open(getUnforwardedCountsPath());

		while (in.available() > 0) {

			String u = in.readUTF() ;

			Long count = in.readLong() ;

			unforwardedCounts.put(Unforwarded.valueOf(u), count) ;
		}

		in.close() ;

	}



	public void finish(Job runningJob) throws IOException {

		super.finish(runningJob) ;

		unforwardedCounts = new HashMap<Unforwarded,Long>() ;

		for (Unforwarded u:Unforwarded.values()) {

			Counter counter = runningJob.getCounters().findCounter(u) ;
			if (counter != null)
				unforwardedCounts.put(u, counter.getValue()) ;
			else
				unforwardedCounts.put(u, 0L) ;
		}

		saveUnforwardedCounts() ;

	}

}
