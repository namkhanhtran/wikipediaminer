package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public abstract class DepthCombinerOrReducer extends Reducer<Integer, PageDepthSummary, AvroKey<Integer>, AvroValue<PageDepthSummary>> {

	public enum Counts {unforwarded, withDepth,withoutDepth} ;
	
	
	public abstract boolean isReducer() ;
	

	@Override
	public void reduce(Integer pageId, Iterable<PageDepthSummary> partials,Context context) throws IOException, InterruptedException {
		
		Integer minDepth = null ;
		boolean depthForwarded = false ;
		
		List<Integer> childIds = new ArrayList<Integer>();
		
		
		for (PageDepthSummary partial:partials) {
				
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
			
			context.write(new AvroKey<Integer>(pageId), new AvroValue<PageDepthSummary>(new PageDepthSummary(minDepth, depthForwarded, childIds)));
			return ;
		}
	
		if (isReducer() ) {
			
			//depth forwarding is only required for pages with children
			if (childIds.isEmpty())
				depthForwarded = true ;
			
			//if we have already forwarded all details to children, then we don't need to keep track of them any more
			if (depthForwarded)
				childIds = new ArrayList<Integer>() ;
			
			//count stuff
			context.getCounter(Counts.withDepth).increment(1);
				
			if (!depthForwarded) 
				context.getCounter(Counts.unforwarded).increment(1);		
		}	

		//InitialDepthMapper.collect(pageId, new PageDepthSummary(minDepth, depthForwarded, childIds), context);	
		context.write(new AvroKey<Integer>(pageId), new AvroValue<PageDepthSummary>(new PageDepthSummary(minDepth, depthForwarded, childIds)));
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
