package org.wikipedia.miner.extract.steps.pageDepth;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public abstract class DepthCombinerOrReducer extends Reducer<AvroKey<Integer>, AvroValue<PageDepthSummary>, AvroKey<Integer>, AvroValue<PageDepthSummary>> {

	public enum Counts {unforwarded, withDepth,withoutDepth} ;
	
	private final PageDepthSummary pds = new PageDepthSummary();
	private final AvroValue<PageDepthSummary> valOut = new AvroValue<PageDepthSummary>();
	
	public abstract boolean isReducer() ;
	

	public void reduce(AvroKey<Integer> pageId, Iterable<AvroValue<PageDepthSummary>> partials,Context context) throws IOException, InterruptedException {
		
		Integer minDepth = null ;
		boolean depthForwarded = false ;
		
		List<Integer> childIds = new ArrayList<Integer>();
		

		for (AvroValue<PageDepthSummary> partialProxy:partials) {
			PageDepthSummary partial = partialProxy.datum();
			
			if (partial.getDepth() != null) {
				if (minDepth == null || minDepth > partial.getDepth())  {
					minDepth = partial.getDepth().intValue() ;
					depthForwarded = partial.getDepthForwarded() ;
				}
			}
			
			if (!partial.getChildIds().isEmpty())
				childIds.addAll(partial.getChildIds()) ;
		}
		
		
		
		
		//if we haven't reached this node yet, just pass on as it is
		if (minDepth == null) {
			
			if (isReducer())
				context.getCounter(Counts.withoutDepth).increment(1);
			
			context.write(pageId, new AvroValue<PageDepthSummary>(new PageDepthSummary(minDepth, depthForwarded, childIds)));
			return ;
		}
	
		if (isReducer() ) {
			
			//depth forwarding is only required for pages with children
			if (childIds.isEmpty())
				depthForwarded = true ;
			
			//if we have already forwarded all details to children, then we don't need to keep track of them any more
			if (depthForwarded)
				childIds.clear();
			
			//count stuff
			context.getCounter(Counts.withDepth).increment(1);
				
			if (!depthForwarded) 
				context.getCounter(Counts.unforwarded).increment(1);		
		}	

		//InitialDepthMapper.collect(pageId, new PageDepthSummary(minDepth, depthForwarded, childIds), context);
		pds.setDepth(minDepth);
		pds.setChildIds(childIds);
		pds.setDepthForwarded(depthForwarded);
		valOut.datum(pds);
		context.write(pageId, valOut);
	}
	
	public static class DepthCombiner extends DepthCombinerOrReducer {

		@Override
		public boolean isReducer() {
			return false;
		}

	}

	public static class DepthReducer extends DepthCombinerOrReducer {

		@Override
		public boolean isReducer() {
			return true;
		}

	}
	
	
}
