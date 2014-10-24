	package org.wikipedia.miner.extract;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.steps.finalSummary.FinalSummaryStep;
import org.wikipedia.miner.extract.steps.labelOccurrences.LabelOccurrenceStep;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.steps.primaryLabel.PrimaryLabelStep;
import org.wikipedia.miner.extract.steps.sortedPages.PageSortingStep;
import org.wikipedia.miner.extract.steps.transstats.TransAndStatsStep;


/**
 * @author dnk2
 *
 * This class extracts summaries (link graphs, etc) from Wikipedia xml dumps. 
 * It calls a sequence of Hadoop Map/Reduce jobs to do so in a scalable, timely fashion.
 * 
 * 
 *  
 */
public class DumpExtractor {

	private Configuration conf ;

	private String[] args ;

	private Path inputFile ;
	private Path langFile ;
	private String lang ;
	private Path sentenceModel ;
	private Path workingDir  ;
	private Path finalDir ;

	private static Logger logger = Logger.getLogger(DumpExtractor.class);

	public static final String KEY_INPUT_FILE = "wm.inputDir" ;
	public static final String KEY_OUTPUT_DIR = "wm.workingDir" ;
	public static final String KEY_LANG_FILE = "wm.langFile" ;
	public static final String KEY_LANG_CODE = "wm.langCode" ;
	public static final String KEY_SENTENCE_MODEL = "wm.sentenceModel" ;

	public static final String LOG_ORPHANED_PAGES = "orphanedPages" ;
	public static final String LOG_WEIRD_LABEL_COUNT = "wierdLabelCounts" ;
	public static final String LOG_MEMORY_USE = "memoryUsage" ;

	public static final String OUTPUT_SITEINFO = "final/siteInfo.xml" ;
	public static final String OUTPUT_PROGRESS = "tempProgress.csv" ;
	public static final String OUTPUT_TEMPSTATS = "tempStats.csv" ;
	public static final String OUTPUT_STATS = "stats.csv" ;
	
	DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss") ;

	public DumpExtractor(String[] args) throws Exception {


		GenericOptionsParser gop = new GenericOptionsParser(args) ;
		conf = gop.getConfiguration() ;

		//outputFileSystem = FileSystem.get(conf);
		this.args = gop.getRemainingArgs() ;

		configure() ;
	}

	public static void main(String[] args) {

		//PropertyConfigurator.configure("log4j.properties");  

		DumpExtractor de;
		try {
			de = new DumpExtractor(args);
			int result = de.run();
			System.exit(result) ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Job configureJob(Job job, String[] args) {

		job.getConfiguration().set(KEY_INPUT_FILE, args[0]) ;
		job.getConfiguration().set(KEY_LANG_FILE, args[1]) ;
		job.getConfiguration().set(KEY_LANG_CODE, args[2]) ;
		job.getConfiguration().set(KEY_SENTENCE_MODEL, args[3]) ;
		job.getConfiguration().set(KEY_OUTPUT_DIR, args[4]) ;
		
		// force one reducer by default. These don't take very long, and multiple reducers would make 
		// finalise file functions more complicated.  
		job.setNumReduceTasks(1) ;

		//many of our tasks require pre-loading lots of data, may as well reuse this as much as we can.
		// conf.setNumTasksToExecutePerJvm(-1) ;

		//conf.setInt("mapred.tasktracker.map.tasks.maximum", 2) ;
		job.getConfiguration().setInt("mapred.tasktracker.reduce.tasks.maximum", 1) ;
		
		//TODO: really don't want this hard coded.
		//job.getConfiguration().set("mapred.child.java.opts", "-Xmx5100m -Dapple.awt.UIElement=true") ;
		// job.getConfiguration().set("mapreduce.child.java.opts", "-Xmx5100m");
		
		job.getConfiguration().set("mapreduce.map.memory.mb", "6100");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "6144");
		
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx6100m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx6144m");

		//conf.setBoolean("mapred.used.genericoptionsparser", true) ;
		
		// This is the nasty thing in MapReduce v2 and YARN: They always prefer their ancient jars first. Set this on to say you don't like it
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

		return job ;
	}



	private FileSystem getFileSystem(Path path) throws IOException {
		return path.getFileSystem(conf) ;
	}


	private Path getPath(String pathStr) {
		return new Path(pathStr) ;
	}


	private FileStatus getFileStatus(Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		return fs.getFileStatus(path) ;
	}


	private void configure() throws Exception {

		if (args.length != 6) 
			throw new IllegalArgumentException("Please specify an xml dump of wikipedia, a language.xml config file,"
					+ " a language code, an openNLP sentence detection model, an hdfs writable working directory, "
					+ "and an output directory") ;

		//check input file
		inputFile = getPath(args[0]); 
		FileStatus fs = getFileStatus(inputFile) ;
		if (fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.READ)) 
			throw new IOException("'" +inputFile + " is not readable or does not exist") ;


		//check lang file and language
		langFile = getPath(args[1]) ;
		lang = args[2] ;
		
		//TODO: should read language here, just to check it is valid
		/*
		Language language = Languages.load(new File(langFile.toString())).get(lang) ;
		if (language == null)
			throw new IOException("Could not load language configuration for '" + lang + "' from '" + langFile + "'") ;
		*/

		sentenceModel = new Path(args[3]) ;
		fs = getFileStatus(sentenceModel) ;
		if (fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.READ)) 
			throw new IOException("'" + sentenceModel + " is not readable or does not exist") ;

		//check working directory
		workingDir = new Path(args[4]) ;

		if (!getFileSystem(workingDir).exists(workingDir))
			getFileSystem(workingDir).mkdirs(workingDir) ;

		fs = getFileStatus(workingDir) ;
		if (!fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.WRITE)) 
			throw new IOException("'" +workingDir + " is not a writable directory") ;

		//set up directory where final data will be placed
		finalDir = new Path(args[5]) ;

		/*
		if (getFileSystem(finalDir).exists(finalDir))
			getFileSystem(finalDir).delete(finalDir, true) ;

		getFileSystem(finalDir).mkdirs(finalDir) ;

		fs = getFileStatus(finalDir) ;
		if (!fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.WRITE)) 
			throw new IOException("'" +workingDir + " is not a writable directory") ;
		*/

	}

	private int run() throws Exception {

		extractSiteInfo() ;
		 		
		
		//extract basic page summaries
	
		int summaryIteration = 0 ;
		PageSummaryStep summaryStep ; 
		while (true) {
			
			//long startTime = System.currentTimeMillis() ;
			
			summaryStep = new PageSummaryStep(workingDir, summaryIteration) ;
			ToolRunner.run(summaryStep, args);
			
			//System.out.println("intitial step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
			
			if (!summaryStep.furtherIterationsRequired())
				break ;
			else
				summaryIteration++ ;
		}
		
		PageSortingStep sortingStep = new PageSortingStep(workingDir, summaryStep) ;
		ToolRunner.run(sortingStep, args);
				
		//calculate page depths
		int depthIteration = 0 ;
		PageDepthStep depthStep ;
		while (true) {
			
			depthStep = new PageDepthStep(workingDir, depthIteration, sortingStep) ;
			ToolRunner.run(depthStep, args);
			
			if (!depthStep.furtherIterationsRequired())
				break ;
			else
				depthIteration++ ;
		}
		
		//gather label senses
		LabelSensesStep sensesStep = new LabelSensesStep(workingDir, sortingStep.getDirName());		
		ToolRunner.run(sensesStep, args);
		
		//gather primary labels
		PrimaryLabelStep primaryLabelStep = new PrimaryLabelStep(workingDir, sensesStep) ;
		ToolRunner.run(primaryLabelStep, args);
		
		//gather label occurrences
		//LabelOccurrenceStep occurrencesStep = new LabelOccurrenceStep(workingDir, sensesStep) ;
		 
		
		// hard-code to test
		// LabelOccurrenceStep occurrencesStep = new LabelOccurrenceStep(workingDir, workingDir.toString() + Path.SEPARATOR + "labelSenses", 13890840) ;
		
		LabelOccurrenceStep occurrencesStep = new LabelOccurrenceStep(workingDir, workingDir.toString() + Path.SEPARATOR + "labelSenses", 
				sensesStep.getTotalLabels()) ;
		ToolRunner.run(occurrencesStep, args);
		
		
		// Make another access to raw file to extract inter-language links and stats
		// TODO: Integrate this step into PageSummaryStep
		// Tuan - 2014-10-21
		TreeMap<String,Long> stats = new TreeMap<String, Long>();
		TransAndStatsStep tsstep = new TransAndStatsStep(workingDir);
		ToolRunner.run(tsstep, args);
		tsstep.updateStats(stats);
		finalizeStatistics(stats);

		//FinalSummaryStep finalStep = new FinalSummaryStep(finalDir, sortingStep, depthStep, primaryLabelStep, sensesStep, occurrencesStep) ;
		FinalSummaryStep finalStep = new FinalSummaryStep(finalDir, 
				workingDir.toString() + Path.SEPARATOR + sortingStep.getDirName(), 
				workingDir.toString() + Path.SEPARATOR + depthStep.getDirName(), 
				workingDir.toString() + Path.SEPARATOR + primaryLabelStep.getDirName(),
				workingDir.toString() + Path.SEPARATOR + sensesStep.getDirName(), 
				workingDir.toString() + Path.SEPARATOR + occurrencesStep.getDirName(),
				workingDir.toString() + Path.SEPARATOR + tsstep.getDirName(),
				sortingStep.getConf()) ;
		
		/*FinalSummaryStep finalStep = new FinalSummaryStep(finalDir, 
				workingDir.toString() + Path.SEPARATOR + "sortedPages", 
				workingDir.toString() + Path.SEPARATOR + "pageDepth_9", 
				workingDir.toString() + Path.SEPARATOR + "primaryLabels",
				workingDir.toString() + Path.SEPARATOR + "labelSenses", 
				workingDir.toString() + Path.SEPARATOR + "labelOccurrences", 
				workingDir.toString() + Path.SEPARATOR + "translations",
				conf);*/
		
		finalStep.run() ;
		
		// System.out.println("Total labels: " + sensesStep.getTotalLabels());		
		
		return 0 ;
	}
	
	private void finalizeStatistics(TreeMap<String, Long> stats) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(
						fs.create(new Path(finalDir.toString() + Path.SEPARATOR + OUTPUT_STATS)))) ;

		for(Map.Entry<String,Long> e:stats.entrySet()) {

			ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

			CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
			cro.writeString(e.getKey(), null) ;
			cro.writeLong(e.getValue(), null) ;

			writer.write(outStream.toString("UTF-8")) ;
			writer.newLine() ;			
		}

		writer.close();
	}

	private void extractSiteInfo() throws IOException {
		
		logger.info("Extracting site info...");

		BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(inputFile).open(inputFile))) ;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(workingDir).create(new Path(workingDir + "/" + OUTPUT_SITEINFO)))) ;

		String line = null;
		boolean startedWriting = false ;

		while ((line = reader.readLine()) != null) {

			if (!startedWriting && line.matches("\\s*\\<siteinfo\\>\\s*")) 
				startedWriting = true ;

			if (startedWriting) {
				writer.write(line) ;
				writer.newLine() ;

				if (line.matches("\\s*\\<\\/siteinfo\\>\\s*"))
					break ;
			}
		}

		reader.close() ;
		writer.close();
	}



}
