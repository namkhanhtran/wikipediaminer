package org.wikipedia.miner.extract.steps.sortedPages;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;

/**
 * In this step we sort page summaries produced by PageSummaryStep by id (they were previously sorted by namespace:title)
 * We also inject titles and namespaces into each page summary (they were previously omitted because they are found in keys, and repeating would be wasteful)
 */
public class PageSortingStep extends Step {
	
	private static Logger logger = Logger.getLogger(PageSortingStep.class) ;
	
	PageSummaryStep finalPageSummaryStep ;

	public PageSortingStep(Path workingDir, PageSummaryStep finalPageSummaryStep) throws IOException {
		super(workingDir);
		this.finalPageSummaryStep = finalPageSummaryStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting page sorting step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			return 0 ;
		} else {
			reset() ;
		}
		
		// JobConf conf = Job.get(PageDepthStep.class);
		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(PageSortingStep.class);
		DumpExtractor.configureJob(job, args) ;

		job.setJobName("WM: sorted pages");
		
		
		FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + finalPageSummaryStep.getDirName());
		AvroJob.setInputKeySchema(job, PageKey.getClassSchema());
		AvroJob.setInputValueSchema(job, PageDetail.getClassSchema());
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		AvroJob.setMapOutputKeySchema(job, Schema.create(Type.INT));
		AvroJob.setMapOutputValueSchema(job, PageDetail.getClassSchema());
		
		AvroJob.setOutputKeySchema(job, Schema.create(Type.INT));
		AvroJob.setOutputValueSchema(job, PageDetail.getClassSchema());
		

		
		FileOutputFormat.setOutputPath(job, getDir());
		
		job.waitForCompletion(true);
	
		if (job.isSuccessful()) {	
			finish(job) ;
			return 0 ;
		}
		
		throw new UncompletedStepException() ;
	}

	@Override
	public String getDirName() {
		return "sortedPages" ;
	}
	
	
	public static class MyMapper extends Mapper<AvroKey<PageKey>, AvroValue<PageDetail>, AvroKey<Integer>, AvroValue<PageDetail>>{
		
		@Override
		public void map(AvroKey<PageKey> pageKey, AvroValue<PageDetail> pageValue, Context context) throws IOException, InterruptedException {
			
			PageKey key = pageKey.datum();
			PageDetail page = pageValue.datum();
			
			
			page.setNamespace(key.getNamespace());
			page.setTitle(key.getTitle());
			
			context.write(new AvroKey<Integer>(page.getId()), new AvroValue<PageDetail>(page));
		}
	}
	
	
	public static class MyReducer extends Reducer<Integer, PageDetail, AvroKey<Integer>, AvroValue<PageDetail>>{
		
		@Override
		public void reduce(Integer pageId, Iterable<PageDetail> pages,Context context) throws IOException, InterruptedException {
			
			for (PageDetail page:pages)
				context.write(new AvroKey<Integer>(page.getId()), new AvroValue<PageDetail>(page));
		}
	}
	
}
