package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;

public abstract class CombinerOrReducer extends Reducer<CharSequence, LabelOccurrences, AvroKey<CharSequence>, AvroValue<LabelOccurrences>> {
	
	public enum Counts {falsePositives, truePositives} ;
	
	public abstract boolean isReducer() ;
	
	@Override
	public void reduce(CharSequence label, Iterable<LabelOccurrences> partials, Context context) throws IOException, InterruptedException {
	
		LabelOccurrences allOccurrences = new LabelOccurrences(0,0,0,0) ;
		
		for (LabelOccurrences partial:partials) {
			allOccurrences.setLinkDocCount(allOccurrences.getLinkDocCount() + partial.getLinkDocCount()) ;
			allOccurrences.setLinkOccCount(allOccurrences.getLinkOccCount() + partial.getLinkOccCount()) ;
			allOccurrences.setTextDocCount(allOccurrences.getTextDocCount() + partial.getTextDocCount()) ;
			allOccurrences.setTextOccCount(allOccurrences.getTextOccCount() + partial.getTextOccCount()) ;
		}
		
		if (isReducer()) {
			
			if (allOccurrences.getLinkOccCount() == 0) {
				context.getCounter(Counts.falsePositives).increment(1L);
				return ; 
			} else {
				context.getCounter(Counts.truePositives).increment(1L);
			}
		}

		context.write(new AvroKey<CharSequence>(label), new AvroValue<LabelOccurrences>(allOccurrences));
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
	
}
