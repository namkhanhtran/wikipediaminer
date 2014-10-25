package org.wikipedia.miner.extract.steps.finalSummary;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;
import org.wikipedia.miner.db.struct.DbIntList;
import org.wikipedia.miner.db.struct.DbLabel;
import org.wikipedia.miner.db.struct.DbLabelForPage;
import org.wikipedia.miner.db.struct.DbLabelForPageList;
import org.wikipedia.miner.db.struct.DbLinkLocation;
import org.wikipedia.miner.db.struct.DbLinkLocationList;
import org.wikipedia.miner.db.struct.DbPage;
import org.wikipedia.miner.db.struct.DbSenseForLabel;
import org.wikipedia.miner.db.struct.DbTranslations;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.model.struct.PrimaryLabels;
import org.wikipedia.miner.extract.steps.LocalStep;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.model.Page.PageType;

public class FinalSummaryStep extends LocalStep {

	private static Logger logger = Logger.getLogger(FinalSummaryStep.class) ;

	/*private PageSortingStep pageSortingStep ;
	private PageDepthStep pageDepthStep ;
	private PrimaryLabelStep primaryLabelStep ;
	private Configuration conf;

	private LabelSensesStep labelSensesStep ;
	private LabelOccurrenceStep labelOccurrenceStep ;*/

	private Path pageSortingDir;
	private Path pageDepthDir;
	private Path primaryLabelDir;
	private Path labelSensesDir;
	private Path labelOccurrenceDir;
	private Path translationDir;

	private Configuration conf;

	private Comparator<DbLabelForPage> labelComparator = new Comparator<DbLabelForPage>() {

		public int compare(DbLabelForPage a, DbLabelForPage b) {

			int cmp = new Long(b.getLinkOccCount()).compareTo(a.getLinkOccCount()) ;
			if (cmp != 0)
				return cmp ;

			cmp = new Long(b.getLinkDocCount()).compareTo(a.getLinkDocCount()) ;
			if (cmp != 0)
				return cmp ;

			return(a.getText().compareTo(b.getText())) ;
		}
	} ;

	private AvroCharSequenceComparator<CharSequence> labelTextComparator = new AvroCharSequenceComparator<CharSequence>() ;



	/*public FinalSummaryStep(Path workingDir, PageSortingStep pageSortingStep, PageDepthStep pageDepthStep, PrimaryLabelStep primaryLabelStep, LabelSensesStep labelSensesStep, LabelOccurrenceStep labelOccurrenceStep) throws IOException {
		super(workingDir);
		this.conf = labelOccurrenceStep.getConf();
		this.pageSortingStep = pageSortingStep ;
		this.pageDepthStep = pageDepthStep ;
		this.primaryLabelStep = primaryLabelStep ;

		this.labelSensesStep = labelSensesStep ;
		this.labelOccurrenceStep = labelOccurrenceStep ;
	}*/

	public FinalSummaryStep(Path workingDir, String pageSortingStep, String pageDepthStep, 
			String primaryLabelStep, String labelSensesStep, String labelOccurrenceStep, 
			String translationDir, Configuration conf) throws IOException {
		super(workingDir, conf);
		this.conf = conf;

		this.pageSortingDir = new Path(pageSortingStep) ;
		this.pageDepthDir = new Path(pageDepthStep);
		this.primaryLabelDir = new Path(primaryLabelStep);

		this.labelSensesDir = new Path(labelSensesStep);
		this.labelOccurrenceDir = new Path(labelOccurrenceStep);
		this.translationDir = new Path(translationDir);
	}

	@Override
	public int run() throws Exception {

		logger.info("Starting final step");

		if (isFinished()) {
			logger.info(" - already completed");
			return 0 ;
		} 
		
		//else
		//	reset() ;

		finalizePageStuff() ;
		finalizeLabelStuff() ;

		// Tuan - 2014-10-21
		finalizeTranslationStuff();

		finish() ;
		return 0 ;
	}



	public void finalizePageStuff() throws IOException {

		BufferedWriter pageWriter = createWriter("page.csv") ;


		BufferedWriter articleParentsWriter = createWriter("articleParents.csv") ;
		BufferedWriter categoryParentsWriter = createWriter("categoryParents.csv") ;
		BufferedWriter childArticlesWriter = createWriter("childArticles.csv") ;
		BufferedWriter childCategoriesWriter = createWriter("childCategories.csv") ;

		BufferedWriter pageLabelWriter = createWriter("pageLabel.csv") ;

		BufferedWriter pageLinkInWriter = createWriter("pageLinkIn.csv") ;
		BufferedWriter pageLinkOutWriter = createWriter("pageLinkOut.csv") ;

		BufferedWriter redirectSourcesByTargetWriter = createWriter("redirectSourcesByTarget.csv") ;
		BufferedWriter redirectTargetsBySourceWriter = createWriter("redirectTargetsBySource.csv") ;

		BufferedWriter sentenceSplitsWriter = createWriter("sentenceSplits.csv") ;


		Path pageDetailPath = getMainAvroResultPath(pageSortingDir) ;
		SeekableInput pageDetailInput = new FsInput(pageDetailPath, conf);

		//Schema pageDetailSchema = Pair.getPairSchema(Schema.create(Type.INT),PageDetail.getClassSchema()) ;
		// DatumReader<Pair<Integer,PageDetail>> pageDetailDatumReader = new SpecificDatumReader<Pair<Integer,PageDetail>>(pageDetailSchema);
		Schema pageDetailSchema = AvroKeyValue.getSchema(Schema.create(Type.INT),PageDetail.getClassSchema());
		DatumReader<AvroKeyValue<Integer,PageDetail>> pageDetailDatumReader = new SpecificDatumReader<AvroKeyValue<Integer,PageDetail>>(pageDetailSchema);
		FileReader<AvroKeyValue<Integer,PageDetail>> pageDetailReader = DataFileReader.openReader(pageDetailInput, pageDetailDatumReader) ;

		Path pageDepthsPath = getMainAvroResultPath(pageDepthDir) ;
		SeekableInput pageDepthsInput = new FsInput(pageDepthsPath, conf);

		//Schema pageDepthsSchema = Pair.getPairSchema(Schema.create(Type.INT),PageDepthSummary.getClassSchema()) ;
		//DatumReader<Pair<Integer,PageDepthSummary>> pageDepthsDatumReader = new SpecificDatumReader<Pair<Integer,PageDepthSummary>>(pageDepthsSchema);
		Schema pageDepthsSchema = AvroKeyValue.getSchema(Schema.create(Type.INT),PageDepthSummary.getClassSchema()) ;
		DatumReader<AvroKeyValue<Integer,PageDepthSummary>> pageDepthsDatumReader = new SpecificDatumReader<AvroKeyValue<Integer,PageDepthSummary>>(pageDepthsSchema);
		FileReader<AvroKeyValue<Integer,PageDepthSummary>> pageDepthsReader = DataFileReader.openReader(pageDepthsInput, pageDepthsDatumReader) ;


		Path primaryLabelPath = getMainAvroResultPath(primaryLabelDir) ;
		SeekableInput primaryLabelInput = new FsInput(primaryLabelPath, conf);

		//Schema primaryLabelSchema = Pair.getPairSchema(Schema.create(Type.INT),PrimaryLabels.getClassSchema()) ;
		//DatumReader<Pair<Integer,PrimaryLabels>> primaryLabelDatumReader = new SpecificDatumReader<Pair<Integer,PrimaryLabels>>(primaryLabelSchema);
		Schema primaryLabelSchema = AvroKeyValue.getSchema(Schema.create(Type.INT),PrimaryLabels.getClassSchema()) ;
		DatumReader<AvroKeyValue<Integer,PrimaryLabels>> primaryLabelDatumReader = new SpecificDatumReader<AvroKeyValue<Integer,PrimaryLabels>>(primaryLabelSchema);
		FileReader<AvroKeyValue<Integer,PrimaryLabels>> primaryLabelReader = DataFileReader.openReader(primaryLabelInput, primaryLabelDatumReader) ;


		//read through pageDetail and pageDepth files simultaneously.
		//both are sorted by id, but pageDepth will be missing many entries.

		AvroKeyValue<Integer,PageDetail> detailPair = null ;
		AvroKeyValue<Integer,PageDepthSummary> depthPair = null ;
		AvroKeyValue<Integer,PrimaryLabels> primaryLabelPair = null ;
		while (pageDetailReader.hasNext()) {

			detailPair = new AvroKeyValue<Integer, PageDetail>((GenericRecord) pageDetailReader.next());
			PageDetail detail = detailPair.getValue() ;

			//identify page depth summary, if there is one
			while ((depthPair == null || depthPair.getKey().intValue() < detailPair.getKey().intValue()) && pageDepthsReader.hasNext())
				depthPair = new AvroKeyValue<Integer, PageDepthSummary>((GenericRecord) pageDepthsReader.next());

			PageDepthSummary depth = null ;
			if (depthPair.getKey().intValue() == (detailPair.getKey().intValue()))
				depth = depthPair.getValue() ;

			//identify primary label summary, if there is one
			while ((primaryLabelPair == null || primaryLabelPair.getKey().intValue() < detailPair.getKey().intValue()) && primaryLabelReader.hasNext())
				primaryLabelPair = new AvroKeyValue<Integer, PrimaryLabels>((GenericRecord) primaryLabelReader.next());

			Set<CharSequence> primaryLabels = new HashSet<CharSequence>() ;
			if (primaryLabelPair.getKey().equals(detailPair.getKey())) 
				primaryLabels.addAll(primaryLabelPair.getValue().getLabels()) ;

			//now we definitely have a page. If we have a depth, then it is synchonized with page

			DbPage page = buildPage(detail, depth) ;

			if (page.getType() == PageType.invalid.ordinal())
				continue ;

			write(detail.getId(), page, pageWriter) ;

			if (detail.getNamespace() == SiteInfo.MAIN_KEY) {

				if (detail.getRedirectsTo() == null) {

					//this is an article or disambig
					DbIntList articleParents = buildIntList(detail.getParentCategories()) ;
					write(detail.getId(),articleParents, articleParentsWriter) ;

					DbLinkLocationList linksIn = buildLinkLocationList(detail.getLinksIn());
					write(detail.getId(), linksIn, pageLinkInWriter) ;

					DbLinkLocationList linksOut = buildLinkLocationList(detail.getLinksOut());
					write(detail.getId(), linksOut, pageLinkOutWriter) ;

					DbIntList redirectSources = buildIntList(detail.getRedirects()) ;
					write(detail.getId(),redirectSources, redirectSourcesByTargetWriter) ;

					DbIntList sentenceSplits = buildIntList(detail.getSentenceSplits()) ;
					write(detail.getId(),sentenceSplits, sentenceSplitsWriter) ;

					DbLabelForPageList labels = buildLabelList(detail, primaryLabels) ;
					write(detail.getId(), labels, pageLabelWriter) ;


				} else {
					//this is a redirect

					redirectTargetsBySourceWriter.write(detail.getId() + "," + detail.getRedirectsTo().getId() + "\n");
				}


			} else if (detail.getNamespace() == SiteInfo.CATEGORY_KEY) {

				if (detail.getRedirectsTo() == null) {

					DbIntList categoryParents = buildIntList(detail.getParentCategories()) ;
					write(detail.getId(),categoryParents, categoryParentsWriter) ;

					DbIntList childArticles = buildIntList(detail.getChildArticles()) ;
					write(detail.getId(),childArticles, childArticlesWriter) ;

					DbIntList childCategories = buildIntList(detail.getChildCategories()) ;
					write(detail.getId(),childCategories, childCategoriesWriter) ;


				} else {

					//TODO: oops, no clean way of dealing with category redirects
				}

			}

		}


		pageWriter.close() ;

		articleParentsWriter.close();
		categoryParentsWriter.close() ;
		childArticlesWriter.close() ;
		childCategoriesWriter.close() ;

		pageLinkInWriter.close();
		pageLinkOutWriter.close();

		redirectSourcesByTargetWriter.close() ;
		redirectTargetsBySourceWriter.close() ;

		sentenceSplitsWriter.close();
		pageLabelWriter.close();

	}

	public void finalizeLabelStuff() throws IOException {


		BufferedWriter labelWriter = createWriter("label.csv") ;

		Path labelSensesPath = getMainAvroResultPath(labelSensesDir) ;
		SeekableInput labelSensesInput = new FsInput(labelSensesPath, conf);

		//Schema labelSensesSchema = Pair.getPairSchema(Schema.create(Type.STRING),LabelSenseList.getClassSchema()) ;
		//DatumReader<Pair<CharSequence,LabelSenseList>> labelSensesDatumReader = new SpecificDatumReader<Pair<CharSequence,LabelSenseList>>(labelSensesSchema);
		Schema labelSensesSchema = AvroKeyValue.getSchema(Schema.create(Type.STRING),LabelSenseList.getClassSchema()) ;
		DatumReader<AvroKeyValue<CharSequence,LabelSenseList>> labelSensesDatumReader = new SpecificDatumReader<AvroKeyValue<CharSequence,LabelSenseList>>(labelSensesSchema);
		FileReader<AvroKeyValue<CharSequence,LabelSenseList>> labelSensesReader = DataFileReader.openReader(labelSensesInput, labelSensesDatumReader) ;

		Path labelOccurrencesPath = getMainAvroResultPath(labelOccurrenceDir) ;
		SeekableInput labelOccurrencesInput = new FsInput(labelOccurrencesPath, conf);

		//Schema labelOccurrencesSchema = Pair.getPairSchema(Schema.create(Type.STRING),LabelOccurrences.getClassSchema()) ;
		//DatumReader<Pair<CharSequence,LabelOccurrences>> labelOccurrencesDatumReader = new SpecificDatumReader<Pair<CharSequence,LabelOccurrences>>(labelOccurrencesSchema);
		Schema labelOccurrencesSchema = AvroKeyValue.getSchema(Schema.create(Type.STRING),LabelOccurrences.getClassSchema()) ;
		DatumReader<AvroKeyValue<CharSequence,LabelOccurrences>> labelOccurrencesDatumReader = new SpecificDatumReader<AvroKeyValue<CharSequence,LabelOccurrences>>(labelOccurrencesSchema);

		FileReader<AvroKeyValue<CharSequence,LabelOccurrences>> labelOccurrencesReader = DataFileReader.openReader(labelOccurrencesInput, labelOccurrencesDatumReader) ;

		AvroKeyValue<CharSequence,LabelSenseList> sensesPair = null ;
		AvroKeyValue<CharSequence,LabelOccurrences> occurrencesPair = null ;
		while (labelSensesReader.hasNext()) {

			sensesPair = new AvroKeyValue<CharSequence, LabelSenseList>((GenericRecord) labelSensesReader.next());
			CharSequence label = sensesPair.getKey() ;
			LabelSenseList senses = sensesPair.getValue() ;

			while ((occurrencesPair == null || labelTextComparator.compare(occurrencesPair.getKey(), sensesPair.getKey()) < 0 ) && labelOccurrencesReader.hasNext())
				occurrencesPair = new AvroKeyValue<CharSequence, LabelOccurrences>((GenericRecord) labelOccurrencesReader.next());

			LabelOccurrences occurrences = null ;
			if (labelTextComparator.compare(occurrencesPair.getKey(), sensesPair.getKey()) == 0)
				occurrences = occurrencesPair.getValue() ;


			//now we definitely have a label and list of senses. If we have occurrences, then they are synchronised with label

			ArrayList<DbSenseForLabel> dbSenses = new ArrayList<DbSenseForLabel>() ;
			for (LabelSense sense:senses.getSenses()) {

				DbSenseForLabel dbSense = new DbSenseForLabel() ;
				dbSense.setId(sense.getId());
				dbSense.setLinkDocCount(sense.getDocCount());
				dbSense.setLinkOccCount(sense.getOccCount());
				dbSense.setFromRedirect(sense.getFromRedirect());
				dbSense.setFromTitle(sense.getFromTitle());

				dbSenses.add(dbSense) ;
			}

			DbLabel dbLabel = new DbLabel() ;
			dbLabel.setSenses(dbSenses);

			if (occurrences != null) {
				dbLabel.setLinkDocCount(occurrences.getLinkDocCount());
				dbLabel.setLinkOccCount(occurrences.getLinkOccCount());
				dbLabel.setTextDocCount(occurrences.getTextDocCount());
				dbLabel.setTextOccCount(occurrences.getTextOccCount());
			}

			write(label, dbLabel, labelWriter) ;

		}

		labelWriter.close();

	}

	/** Output the translation file */
	private void finalizeTranslationStuff() throws IOException {
		BufferedWriter transWriter = createWriter("translations.csv") ;
		Path transPath = getMainAvroResultPath(translationDir) ;
		FileSystem dfs = transPath.getFileSystem(conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(transPath));

		IntWritable keyIn = new IntWritable();
		DbTranslations valIn = new DbTranslations();

		while (reader.next(keyIn, valIn)) {
			int k = keyIn.get();
			write(k, valIn, transWriter);
		}
		transWriter.close();
	}

	private void write(Integer id, Record record, BufferedWriter writer) throws IOException {

		ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

		CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
		cro.writeInt(id, "pageId") ;
		record.serialize(cro) ;

		writer.write(outStream.toString("UTF-8")) ;
	}

	private void write(int id, Record record, BufferedWriter writer) throws IOException {

		ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

		CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
		cro.writeInt(id, "pageId") ;
		record.serialize(cro) ;

		writer.write(outStream.toString("UTF-8")) ;
	}

	private void write(CharSequence label, Record record, BufferedWriter writer) throws IOException {

		ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

		CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
		cro.writeString(label.toString(), "labelText");
		record.serialize(cro) ;

		writer.write(outStream.toString("UTF-8")) ;
	}


	private DbPage buildPage(PageDetail detail, PageDepthSummary depth) {

		DbPage dbPage = new DbPage() ;
		dbPage.setType(getType(detail).ordinal());
		dbPage.setTitle(detail.getTitle().toString());

		if (depth != null && depth.getDepth() != null)
			dbPage.setDepth(depth.getDepth());
		else
			dbPage.setDepth(-1);

		return dbPage ;
	}

	private DbIntList buildIntList(List<PageSummary> summaries) {

		ArrayList<Integer> ids = new ArrayList<Integer>(summaries.size()) ;
		for (PageSummary summary:summaries)
			ids.add(summary.getId()) ;

		return new DbIntList(ids) ;
	}

	private DbIntList buildIntList(Collection<Integer> values) {

		ArrayList<Integer> ints = new ArrayList<Integer>() ;
		for (Integer value:values)
			ints.add(value) ;

		return new DbIntList(ints) ;
	}

	private DbLabelForPageList buildLabelList(PageDetail page, Set<CharSequence> primaryLabels) {

		ArrayList<DbLabelForPage> dbLabels = new ArrayList<DbLabelForPage>() ;

		Set<CharSequence> redirectTitles = new HashSet<CharSequence>() ;
		for (PageSummary redirect:page.getRedirects())
			redirectTitles.add(redirect.getTitle()) ;

		for (Map.Entry<CharSequence, LabelSummary>e:page.getLabels().entrySet()) {

			CharSequence text = e.getKey() ;
			LabelSummary detail = e.getValue() ;

			DbLabelForPage label = new DbLabelForPage() ;
			label.setText(text.toString());
			label.setLinkDocCount(detail.getDocCount());
			label.setLinkOccCount(detail.getOccCount());

			label.setFromRedirect(redirectTitles.contains(text));
			label.setFromTitle(page.getTitle().equals(text));

			label.setIsPrimary(primaryLabels.contains(text));

			dbLabels.add(label) ;
		}

		Collections.sort(dbLabels, labelComparator) ;

		return new DbLabelForPageList(dbLabels) ;
	}

	private DbLinkLocationList buildLinkLocationList(List<LinkSummary> summaries) {

		ArrayList<DbLinkLocation> links = new ArrayList<DbLinkLocation>() ;
		for (LinkSummary summary:summaries) {
			DbLinkLocation link = new DbLinkLocation() ;
			link.setLinkId(summary.getId());

			ArrayList<Integer> sentenceIndexes = new ArrayList<Integer>() ;
			sentenceIndexes.addAll(summary.getSentenceIndexes()) ;

			link.setSentenceIndexes(sentenceIndexes);

			links.add(link) ;
		}

		return new DbLinkLocationList(links) ;
	}

	private PageType getType(PageDetail detail) {

		if (detail.getNamespace() == SiteInfo.MAIN_KEY) {

			if (detail.getRedirectsTo() == null) {

				if (detail.getIsDisambiguation())
					return PageType.disambiguation ;
				else
					return PageType.article ;

			} else {
				return PageType.redirect ;
			}

		} else if (detail.getNamespace() == SiteInfo.CATEGORY_KEY) {

			if (detail.getRedirectsTo() == null) {
				return PageType.category ;
			} else {
				//TODO: oops, we don't have a good way to deal with redirects of categories
				return PageType.invalid ;
			}
		} else if (detail.getNamespace() == SiteInfo.TEMPLATE_KEY) {
			return PageType.template ;
		} else {
			return PageType.invalid ;
		}
	}

	private BufferedWriter createWriter(String fileName) throws IOException {

		FileSystem fs = getDir().getFileSystem(conf) ;

		FSDataOutputStream stream = fs.create(new Path(getDir() + Path.SEPARATOR + fileName)) ;
		OutputStreamWriter streamWriter = new OutputStreamWriter(stream) ;

		return new BufferedWriter(streamWriter) ;
	}

	/*private Path getMainAvroResultPath(Step step) throws IOException {

		FileSystem fs = step.getDir().getFileSystem(step.getConf()) ;

		FileStatus[] fileStatuses = fs.listStatus(step.getDir(), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith("part-") ;
			}
		}) ;

		if (fileStatuses.length == 0)
			throw new IOException("Could not locate main result file in " + step.getDir()) ;

		if (fileStatuses.length > 1)
			throw new IOException("Too many result files (so too many reducers) in " + step.getDir()) ;

		return fileStatuses[0].getPath() ;
	}*/

	private Path getMainAvroResultPath(Path stepDir) throws IOException {

		FileSystem fs = stepDir.getFileSystem(conf) ;

		FileStatus[] fileStatuses = fs.listStatus(stepDir, new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith("part-") ;
			}
		}) ;

		if (fileStatuses.length == 0)
			throw new IOException("Could not locate main result file in " + stepDir) ;

		if (fileStatuses.length > 1)
			throw new IOException("Too many result files (so too many reducers) in " + stepDir) ;

		return fileStatuses[0].getPath() ;
	}
}
