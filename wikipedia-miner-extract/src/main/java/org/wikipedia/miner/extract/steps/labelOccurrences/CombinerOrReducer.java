package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;

public abstract class CombinerOrReducer extends Reducer<AvroKey<CharSequence>, AvroValue<LabelOccurrences>, AvroKey<CharSequence>, AvroValue<LabelOccurrences>> {
	
	public enum Counts {falsePositives, truePositives} ;
	
	public abstract boolean isReducer() ;
	
	private final LabelOccurrences allOccurrences = new LabelOccurrences(0,0,0,0) ;
	private final AvroValue<LabelOccurrences> valOut = new AvroValue<LabelOccurrences>();
		
	public void reduce(AvroKey<CharSequence> label, Iterable<AvroKey<LabelOccurrences>> partials, Context context) throws InterruptedException, IOException {
	
		// LabelOccurrences allOccurrences = new LabelOccurrences(0,0,0,0) ;
		
		for (AvroKey<LabelOccurrences> partialProxy:partials) {
			LabelOccurrences partial = partialProxy.datum();
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

		valOut.datum(allOccurrences);
		// context.write(new AvroKey<CharSequence>(label.toString()), new AvroValue<LabelOccurrences>(allOccurrences));
		context.write(label, valOut);
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
