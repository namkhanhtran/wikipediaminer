package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.Counts;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.MyCombiner;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.MyReducer;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;
import org.wikipedia.miner.extract.util.XmlInputFormat;

public class LabelOccurrenceStep extends Step{
	
	private static Logger logger = Logger.getLogger(LabelOccurrenceStep.class) ;
	
	public static final String KEY_TOTAL_LABELS = "wm.totalLabels" ;

	private Map<Counts,Long> counts ;
	private LabelSensesStep sensesStep ;
	
	public LabelOccurrenceStep(Path workingDir, LabelSensesStep sensesStep) throws IOException {
		super(workingDir);
		
		this.sensesStep = sensesStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting label occurrence step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			loadCounts() ;
			
			return 0 ;
		} else
			reset() ;
		
		
		// JobConf conf = new JobConf(LabelOccurrenceStep.class);
		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(LabelOccurrenceStep.class);
		DumpExtractor.configureJob(job, args) ;
		
		if (sensesStep.getTotalLabels() >= Integer.MAX_VALUE)
			throw new Exception("Waay to many distinct labels (this must be less than " + Integer.MAX_VALUE + ")") ;
		
		conf.setInt(KEY_TOTAL_LABELS, (int)sensesStep.getTotalLabels());

		job.setJobName("WM: label occurrences");

			
		job.setMapperClass(MyMapper.class);
		/*job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(AvroValue.class);*/

			
		job.setInputFormatClass(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
			
		FileInputFormat.setInputPaths(job, conf.get(DumpExtractor.KEY_INPUT_FILE));
		
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
		
		
		for (FileStatus fs:FileSystem.get(conf).listStatus(sensesStep.getDir())) {

			if (fs.getPath().getName().startsWith("part-")) {
				Logger.getLogger(LabelOccurrenceStep.class).info("Cached labels file " + fs.getPath()) ;
				DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
			}
		}
		
		job.setCombinerClass(MyCombiner.class) ;
		job.setReducerClass(MyReducer.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setOutputValueSchema(job,LabelOccurrences.getClassSchema());
		
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
		return "labelOccurrences" ;
	}

	private Path getUnforwardedCountsPath() {
		return new Path(getDir() + Path.SEPARATOR + "unforwarded") ;
	}
	
	private void saveCounts() throws IOException {
		FSDataOutputStream out = getHdfs().create(getUnforwardedCountsPath());
		
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
		
		FSDataInputStream in = getHdfs().open(getUnforwardedCountsPath());
		
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

		for (Counts c:Counts.values()) {
			
			Counter counter = runningJob.getCounters().findCounter(c) ;
			if (counter != null)
				counts.put(c, counter.getValue()) ;
			else
				counts.put(c, 0L) ;
		}
		
		saveCounts() ;
		
	}
	
}
