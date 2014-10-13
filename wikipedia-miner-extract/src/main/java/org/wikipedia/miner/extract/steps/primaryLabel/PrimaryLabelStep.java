package org.wikipedia.miner.extract.steps.primaryLabel;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.PrimaryLabels;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;


public class PrimaryLabelStep extends Step {

	private static Logger logger = Logger.getLogger(PrimaryLabelStep.class) ;
	
	private LabelSensesStep labelSensesStep ;
	
	public PrimaryLabelStep(Path workingDir, LabelSensesStep labelSensesStep) throws IOException {
		super(workingDir);
		
		this.labelSensesStep = labelSensesStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting primary label step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			
			return 0 ;
		} else {
			reset() ;
		}
		
		//JobConf conf = new JobConf(PageDepthStep.class);
		Job job = Job.getInstance(getConf());
		job.setJarByClass(PrimaryLabelStep.class);
		Configuration conf = job.getConfiguration();
		
		DumpExtractor.configureJob(job, args) ;

		job.setJobName("WM: primary labels");
		
		
		FileInputFormat.setInputPaths(job, getWorkingDir() + Path.SEPARATOR + labelSensesStep.getDirName());
		AvroJob.setInputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setInputValueSchema(job, LabelSenseList.getClassSchema());
			
		AvroJob.setOutputKeySchema(job, Schema.create(Type.INT));
		AvroJob.setOutputValueSchema(job, PrimaryLabels.getClassSchema());
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		
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
		return "primaryLabels" ;
	}
	
	public static class MyMapper extends Mapper<AvroKey<CharSequence>, AvroValue<LabelSenseList>, AvroKey<Integer>, AvroValue<PrimaryLabels>>{
		
		@Override
		public void map(AvroKey<CharSequence> pageKey, AvroValue<LabelSenseList> pageValue, Context context) throws IOException, InterruptedException {
			
			CharSequence label = pageKey.datum();
			LabelSenseList senses = pageValue.datum();
			
			if (senses.getSenses().isEmpty())
				return ;
			
			LabelSense firstSense = senses.getSenses().get(0) ;
			
			ArrayList<CharSequence> primaryLabels = new ArrayList<CharSequence>() ;
			primaryLabels.add(label) ;
			
			context.write(new AvroKey<Integer>(firstSense.getId()), new AvroValue<PrimaryLabels>(new PrimaryLabels(primaryLabels)));
		}
	}
	
	public static class MyReducer extends Reducer<Integer, PrimaryLabels, AvroKey<Integer>, AvroValue<PrimaryLabels>>{
		
		@Override
		public void reduce(Integer pageId, Iterable<PrimaryLabels> partials,Context context) throws IOException, InterruptedException {
			
			ArrayList<CharSequence> primaryLabels = new ArrayList<CharSequence>() ;
			
			for (PrimaryLabels partial:partials) {
				
				PrimaryLabels clone = PrimaryLabels.newBuilder(partial).build() ;
				primaryLabels.addAll(clone.getLabels()) ;
			}
			
			context.write(new AvroKey<Integer>(pageId), new AvroValue<PrimaryLabels>(new PrimaryLabels(primaryLabels)));
		}
	}
	
}
