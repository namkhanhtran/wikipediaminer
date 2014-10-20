package org.wikipedia.miner.extract.steps.lang;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.wikipedia.miner.db.struct.DbTranslations;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.PrimaryLabels;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.steps.pageSummary.InitialMapper;
import org.wikipedia.miner.extract.util.UncompletedStepException;
import org.wikipedia.miner.extract.util.XmlInputFormat;


public class LangStep extends Step {

	public LangStep(Path workingDir) throws IOException {
		super(workingDir);
	}

	private static Logger logger = Logger.getLogger(LangStep.class) ;
		
	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting language resolving step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			
			return 0 ;
		} else {
			reset() ;
		}
		
		//JobConf conf = new JobConf(PageDepthStep.class);
		Job job = Job.getInstance(getConf());
		job.setJarByClass(LangStep.class);
		Configuration conf = job.getConfiguration();
		
		DumpExtractor.configureJob(job, args) ;

		job.setJobName("WM: Language ");
		
		
		job.setMapperClass(InitialMapper.class);
		
		/*job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(AvroValue.class);*/

		job.setInputFormatClass(XmlInputFormat.class);
		job.getConfiguration().set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		job.getConfiguration().set(XmlInputFormat.END_TAG_KEY, "</page>") ;

		FileInputFormat.setInputPaths(job, conf.get(DumpExtractor.KEY_INPUT_FILE));
		DistributedCache.addCacheFile(new Path(job.getConfiguration()
				.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);
		
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
			
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DbTranslations.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DbTranslations.class);
				
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(Reducer.class);
		
		FileOutputFormat.setOutputPath(job, getDir());
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (job.isSuccessful()) {	
			finish(job) ;
			return 0 ;
		}
		
		throw new UncompletedStepException() ;
	}

	@Override
	public String getDirName() {
		return "languages" ;
	}
	
}
