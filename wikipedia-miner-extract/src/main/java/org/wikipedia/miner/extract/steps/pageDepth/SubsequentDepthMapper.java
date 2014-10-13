package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public class SubsequentDepthMapper extends Mapper<AvroKey<Integer>, AvroValue<PageDepthSummary>, AvroKey<Integer>, AvroValue<PageDepthSummary>> {


	@Override
	public void map(AvroKey<Integer> pageKey, AvroValue<PageDepthSummary> pageValue,Context context) throws IOException, InterruptedException {
		
	
		Integer id = pageKey.datum();
		PageDepthSummary depthSummary = pageValue.datum();
		
		
		if (depthSummary.getDepthForwarded()) {
			//if we have already processed this in previous iterations, just pass it along directly
			context.write(new AvroKey<Integer>(id), new AvroValue<PageDepthSummary>(depthSummary));
			return ;
		}
		
		if (depthSummary.getDepth() == null) { 
			//if we haven't reached this node yet, just pass it along directly
			context.write(new AvroKey<Integer>(id), new AvroValue<PageDepthSummary>(depthSummary));
			return ;
		}
	
		InitialDepthMapper.shareDepth(depthSummary, context) ;		
		context.write(new AvroKey<Integer>(id), new AvroValue<PageDepthSummary>(depthSummary));
	}
	
}
