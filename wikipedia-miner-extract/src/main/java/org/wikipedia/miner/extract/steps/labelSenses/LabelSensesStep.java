package org.wikipedia.miner.extract.steps.labelSenses;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelSenses.CombinerOrReducer.Counts;
import org.wikipedia.miner.extract.steps.labelSenses.CombinerOrReducer.MyCombiner;
import org.wikipedia.miner.extract.steps.labelSenses.CombinerOrReducer.MyReducer;
import org.wikipedia.miner.extract.steps.sortedPages.PageSortingStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;

public class LabelSensesStep extends Step {

	private static Logger logger = Logger.getLogger(LabelSensesStep.class) ;
	
	private PageSortingStep finalPageSummaryStep ;
	private Map<Counts,Long> counts ;
	
	public LabelSensesStep(Path workingDir, PageSortingStep finalPageSummaryStep) throws IOException {
		super(workingDir);
		this.finalPageSummaryStep = finalPageSummaryStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting label senses step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			loadCounts() ;
			return 0 ;
		} else {
			reset() ;
		}
		
		// JobConf conf = new JobConf(LabelSensesStep.class);
		Job job = Job.getInstance(getConf());
		job.setJarByClass(LabelSensesStep.class);
		Configuration conf = job.getConfiguration();
		DumpExtractor.configureJob(job, args) ;

		job.setJobName("WM: label senses");
		
		FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + finalPageSummaryStep.getDirName());
		AvroJob.setInputKeySchema(job, Schema.create(Type.INT));
		AvroJob.setInputValueSchema(job, PageDetail.getClassSchema());
				
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class) ;
		job.setReducerClass(MyReducer.class);
	
		AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setMapOutputValueSchema(job, LabelSenseList.getClassSchema());
		
		AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setOutputValueSchema(job, LabelSenseList.getClassSchema());
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
		return "labelSenses" ;
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

	public long getTotalLabels() {
		return counts.get(Counts.ambiguous) + counts.get(Counts.unambiguous) ;
	}
	
}
