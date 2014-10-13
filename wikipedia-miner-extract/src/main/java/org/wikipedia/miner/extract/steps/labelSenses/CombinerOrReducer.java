package org.wikipedia.miner.extract.steps.labelSenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;


public abstract class CombinerOrReducer extends Reducer<CharSequence, LabelSenseList, AvroKey<CharSequence>, AvroValue<LabelSenseList>> {
	
	public enum Counts {ambiguous, unambiguous} ;
	
	public abstract boolean isReducer() ;
	
	
	
	
	@Override
	public void reduce(CharSequence label, Iterable<LabelSenseList> senseLists, Context context) throws IOException, InterruptedException {
	
		LabelSenseList allSenses = new LabelSenseList() ;
		allSenses.setSenses(new ArrayList<LabelSense>()) ;
		
		for (LabelSenseList senses:senseLists) {
			
			for (LabelSense sense:senses.getSenses()) {
				allSenses.getSenses().add(LabelSense.newBuilder(sense).build()) ;
			}
		}
		
		
		if (isReducer()) {
			
			if (allSenses.getSenses().size() > 1)
				context.getCounter(Counts.ambiguous).increment(1L);
			else
				context.getCounter(Counts.unambiguous).increment(1L);
			
			Collections.sort(allSenses.getSenses(), new SenseComparator());
			
		}
		
		context.write(new AvroKey<CharSequence>(label), new AvroValue<LabelSenseList>(allSenses));
	}

	public static class MyCombiner extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return false;
		}

	}

	public static class MyReducer extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return true;
		}

	}
	
	private static class SenseComparator implements Comparator<LabelSense> {

		@Override
		public int compare(LabelSense a, LabelSense b) {
			
			int cmp = b.getDocCount().compareTo(a.getDocCount()) ;
			
			if (cmp != 0) 
				return cmp ;
			
			cmp = b.getOccCount().compareTo(a.getOccCount()) ;
			
			if (cmp != 0)
				return cmp ;
			
			
			return a.getId().compareTo(b.getId());
			
			
		}
		
	}
	
	
}