package org.wikipedia.miner.extract.util;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * Small utility program for converting TextFormat into KeyValueFormat in Hadoop 
 */
public class AvroFormatConverter extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(AvroFormatConverter.class);
	/**
	 * Parameters: Input of the files, output directory, schemas for keys and values, input format 
	 */
	private static final String KEY_SCHEMA_OPT = "key";
	private static final String VALUE_SCHEMA_OPT = "value";
	private static final String INPUT_OPT = "in";
	private static final String OUTPUT_OPT = "out";
	private static final String INPUT_FORMAT_OPT = "informat"; 
	private static final String REMOVE_OUTPUT = "rmo";
	
	private static final String INPUT_FORMAT_CLASS = "input.format.class"; 
	private static final String KEY_SCHEMA = "key.schema";
	private static final String VALUE_SCHEME = "value.schema";
	
	private static final class MyMapper<KEYIN, KEYOUT, VALOUT> extends Mapper<KEYIN, Text, 
			AvroKey<KEYOUT>, AvroValue<VALOUT>> {
		
		private AvroKey<KEYOUT> keyOut = new AvroKey();
		private AvroValue<VALOUT> valOut = new AvroValue();
		private boolean textInputFormatted;
		
		private Schema keySchema;
		private Schema valueSchema;
		
		private DatumReader<KEYOUT> keyReader;
		private DatumReader<VALOUT> valReader;		
		
		private static final DecoderFactory decoderFactory = DecoderFactory.get();
			
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			
			textInputFormatted = (conf.get(INPUT_FORMAT_CLASS) == null);
			String schemaName = conf.get(KEY_SCHEMA);
			keySchema = inferSchema(schemaName);
			keyReader = new GenericDatumReader(keySchema);
			
			schemaName = conf.get(VALUE_SCHEME);
			valueSchema = inferSchema(schemaName);
			valReader = new GenericDatumReader(valueSchema);
		}

		@Override
		protected void map(KEYIN keyObj, Text valueObj, Context context)
				throws IOException, InterruptedException {
			String[] kvs = getKeyValue(keyObj, valueObj);
			
			JsonDecoder keyDecoder = decoderFactory.jsonDecoder(keySchema, kvs[0]);
			KEYOUT keyData = keyReader.read(null, keyDecoder);
			keyOut.datum(keyData);
			
			JsonDecoder valDecoder = decoderFactory.jsonDecoder(valueSchema, kvs[1]);
			VALOUT valData = valReader.read(null, valDecoder);
			valOut.datum(valData);
					
			context.write(keyOut, valOut);
		}	
		
		private String[] getKeyValue(KEYIN key, Text value) {
			String[] res = new String[2];
			String k,v;
			if (textInputFormatted) {
				v = value.toString();
				int i = v.indexOf('\t');
				k = v.substring(0, i);
				v = v.substring(i + 1);
				
			} else {
				k = key.toString();
				v = value.toString();				
			}
			res[0] = k;
			res[1] = v;
			return res;
		}
	}	 
	
	@SuppressWarnings("static-access")
	/**
	 * Extra options: Seed file path, begin time, end time
	 * 
	 */
	public Options options() {
		Options opts = new Options();

		Option inputOpt = OptionBuilder.withArgName("input-path").hasArg(true)
				.withDescription("input file / directory path (required)")
				.create(INPUT_OPT);

		Option outputOpt = OptionBuilder.withArgName("output-path").hasArg(true)
				.withDescription("output file path (required)")
				.create(OUTPUT_OPT);	
		
		Option keyOpt = OptionBuilder.withArgName("key-class").hasArg(true)
				.withDescription("key class").create(KEY_SCHEMA_OPT);
		
		Option valOpt = OptionBuilder.withArgName("value-class").hasArg(true)
				.withDescription("v").create(VALUE_SCHEMA_OPT);
		
		Option rmOpt = OptionBuilder.withArgName("remove-out").hasArg(false)
				.withDescription("remove the output then create again before writing files onto it")
				.create(REMOVE_OUTPUT);
		
		Option inpFormatOpt = OptionBuilder.withArgName("input-format").hasArg(true)
				.withDescription("InputFormat type").create(INPUT_FORMAT_OPT);	
				
		opts.addOption(inputOpt);
		opts.addOption(outputOpt);
		opts.addOption(keyOpt);
		opts.addOption(valOpt);
		opts.addOption(inpFormatOpt);
		opts.addOption(rmOpt);
		return opts;
	}	
	
	@Override
	public int run(String[] args) throws Exception {

		Options opts = options();
		CommandLineParser parser = new GnuParser();
		CommandLine command = null;
		try {
			command = parser.parse(opts, args);
		} catch (ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			return -1;
		}

		if (!command.hasOption(INPUT_OPT) || !command.hasOption(OUTPUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
		if (!command.hasOption(KEY_SCHEMA_OPT)) {
			LOG.error("Key class must be specified");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}		
		
		if (!command.hasOption(VALUE_SCHEMA_OPT)) {
			LOG.error("Value lass must be specified");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}		
			
		String input = command.getOptionValue(INPUT_OPT);
		String output = command.getOptionValue(OUTPUT_OPT);
		
		Job job = Job.getInstance(getConf());
		job.setJobName(AvroFormatConverter.class + ": " + input);
		job.setJarByClass(AvroFormatConverter.class);
				
		/*job.getConfiguration().set("mapreduce.map.memory.mb", "6100");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "6144");
		
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx6100m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx6144m");*/
		
		// This is the nasty thing in MapReduce v2 and YARN: They always prefer their ancient jars first. Set this on to say you don't like it
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
		
		Path ip = new Path(input);
		Path op = new Path(output);

		if (command.hasOption(REMOVE_OUTPUT)) {		
			FileSystem fs = FileSystem.get(getConf());
			fs.delete(op, true);
		}

		FileInputFormat.setInputPaths(job, ip);
		FileOutputFormat.setOutputPath(job, op);
		
		if (!command.hasOption(INPUT_FORMAT_OPT)) {
			job.setInputFormatClass(TextInputFormat.class);
		} else {
			Class inputFormatClass = Class.forName(command.getOptionValue(INPUT_FORMAT_OPT));
			job.setInputFormatClass(inputFormatClass);
			job.getConfiguration().set(INPUT_FORMAT_CLASS, command.getOptionValue(INPUT_FORMAT_OPT));
		}
		
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(Reducer.class);
		
		/* force one reducer by default. These don't take very long, 
		and multiple reducers would make finalise file functions more complicated. */  
		job.setNumReduceTasks(1) ;
		
		String keySchemaName = command.getOptionValue(KEY_SCHEMA_OPT);
		Schema keySchema = inferSchema(keySchemaName);
		AvroJob.setMapOutputKeySchema(job, keySchema);
		AvroJob.setOutputKeySchema(job, keySchema);
		job.getConfiguration().set(KEY_SCHEMA, keySchemaName);
		
		String valueSchemaName = command.getOptionValue(VALUE_SCHEMA_OPT);
		Schema valSchema = inferSchema(valueSchemaName);
		AvroJob.setMapOutputValueSchema(job, valSchema);
		AvroJob.setOutputValueSchema(job, valSchema);
		job.getConfiguration().set(VALUE_SCHEME, valueSchemaName);
				
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		return 0;
	}
	
	private static Schema inferSchema(String schemaName) {
		
		// Try loading from standard schemas first
		Class schemaClass = Schema.class;
		try {
			Field schemaField = schemaClass.getDeclaredField(schemaName);
			return (Schema) schemaField.get(null);
		} 
		
		// Nothing found in standard schemas, go for avro-tools generated classes
		catch (Exception e) {
		    try {
				schemaClass = Class.forName(schemaName);
				Field schemaField = schemaClass.getDeclaredField("SCHEMA$");
			    return (Schema) schemaField.get(null);
			} catch (Exception e1) {
				e1.printStackTrace();
				throw new RuntimeException("No Avro data type for schema: " + schemaName + " found");
			}   
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new AvroFormatConverter(),args);
		} catch (Exception e) {		
			e.printStackTrace();
		}
	}
}
