package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
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
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.steps.IterativeStep;
import org.wikipedia.miner.extract.steps.pageDepth.DepthCombinerOrReducer.Counts;
import org.wikipedia.miner.extract.steps.pageDepth.DepthCombinerOrReducer.DepthCombiner;
import org.wikipedia.miner.extract.steps.pageDepth.DepthCombinerOrReducer.DepthReducer;
import org.wikipedia.miner.extract.steps.sortedPages.PageSortingStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;


public class PageDepthStep extends IterativeStep {

	private static Logger logger = Logger.getLogger(PageDepthStep.class) ;
	
	private PageSortingStep finalPageSummaryStep ;

	private Map<Counts,Long> counts ;



	public PageDepthStep(Path workingDir, int iteration, PageSortingStep finalPageSummaryStep) throws IOException {
		super(workingDir, iteration);

		this.finalPageSummaryStep = finalPageSummaryStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting page depth step (iteration " + getIteration() + ")");
		
		if (isFinished()) {
			logger.info(" - already completed");
			loadCounts() ;
			return 0 ;
		} else {
			reset() ;
		}
		
		//JobConf conf = new JobConf(PageDepthStep.class);
		Job job = Job.getInstance(getConf());
		job.setJarByClass(PageDepthStep.class);
		Configuration conf = job.getConfiguration();
		DumpExtractor.configureJob(job, args) ;

		job.setJobName("WM: page depth (" + getIteration() + ")");
		
		if (getIteration() == 0) {
		
			FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + finalPageSummaryStep.getDirName());
			AvroJob.setInputKeySchema(job, Schema.create(Type.INT));
			AvroJob.setInputValueSchema(job, PageDetail.getClassSchema());
			
			DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
			
			job.setMapperClass(InitialDepthMapper.class);
			
		} else {
			
			FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + getDirName(getIteration()-1));
			AvroJob.setInputKeySchema(job, Schema.create(Type.INT));
			AvroJob.setInputValueSchema(job, PageDepthSummary.getClassSchema());
			
			job.setMapperClass(SubsequentDepthMapper.class);
		}
			
		AvroJob.setOutputKeySchema(job, Schema.create(Type.INT));
		AvroJob.setOutputValueSchema(job, PageDepthSummary.getClassSchema());				
		job.setCombinerClass(DepthCombiner.class) ;
		job.setReducerClass(DepthReducer.class);
		
		FileOutputFormat.setOutputPath(job, getDir());
		
		job.waitForCompletion(true);	
		if (job.isSuccessful()) {	
			finish(job) ;
			return 0 ;
		}
		
		throw new UncompletedStepException() ;
	}

	public boolean furtherIterationsRequired() {
		return counts.get(Counts.unforwarded) > 0 ;
	}


	@Override
	public String getDirName(int iteration) {
		return "pageDepth_" + iteration ;
	}

	private Path getCountsPath() {
		return new Path(getDir() + Path.SEPARATOR + "counts") ;
	}

	
	
	private void saveCounts() throws IOException {
		FSDataOutputStream out = getHdfs().create(getCountsPath());
		
		for (Counts c:Counts.values()) {
			
			out.writeUTF(c.name()) ;
			
			Long count = counts.get(c) ;
			if (count != null)
				out.writeLong(count) ;
			else
				out.writeLong(0L) ;
		}
		
		out.close();
	}
	
	private void loadCounts() throws IOException {
		
		counts = new HashMap<Counts,Long>() ;
		
		
		FSDataInputStream in = getHdfs().open(getCountsPath());
		
		while (in.available() > 0) {
			
			String c = in.readUTF() ;
			
			Long count = in.readLong() ;
			
			counts.put(Counts.valueOf(c), count) ;
		}
	
		in.close() ;
		
	}



	public void finish(Job runningJob) throws IOException {

		super.finish(runningJob) ;

		counts = new HashMap<Counts,Long>() ;

		for (Counts count:Counts.values()) {
			
			Counter counter = runningJob.getCounters().findCounter(count) ;
			if (counter != null)
				counts.put(count, counter.getValue()) ;
			else
				counts.put(count, 0L) ;
		}

		saveCounts() ;

	}
}
