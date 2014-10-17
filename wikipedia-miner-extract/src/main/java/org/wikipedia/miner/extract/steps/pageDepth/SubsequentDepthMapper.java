package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public class SubsequentDepthMapper extends Mapper<AvroKey<Integer>, AvroValue<PageDepthSummary>, AvroKey<Integer>, AvroValue<PageDepthSummary>> {

	private final AvroValue<PageDepthSummary> valOut = new AvroValue<PageDepthSummary>();
	private final AvroKey<Integer> keyOut = new AvroKey<Integer>();
	
	@Override
	public void map(AvroKey<Integer> pageKey, AvroValue<PageDepthSummary> pageValue,Context context) throws IOException, InterruptedException {
		
		PageDepthSummary depthSummary = pageValue.datum();
		
		
		if (depthSummary.getDepthForwarded()) {
			//if we have already processed this in previous iterations, just pass it along directly
			context.write(pageKey, new AvroValue<PageDepthSummary>(depthSummary));
			return ;
		}
		
		if (depthSummary.getDepth() == null) { 
			//if we haven't reached this node yet, just pass it along directly
			context.write(pageKey, new AvroValue<PageDepthSummary>(depthSummary));
			return ;
		}
	
		shareDepth(depthSummary, context, keyOut, valOut) ;
		valOut.datum(depthSummary);
		context.write(pageKey, valOut);
	}
	
	public static void shareDepth(final PageDepthSummary page, final Context context,  AvroKey<Integer> keyOut,  AvroValue<PageDepthSummary> valOut) throws IOException, InterruptedException {

		if (page.getDepth() == null)
			return ;

		if (page.getDepthForwarded())
			return ;

		//logger.info("sharing depths for " + page.getTitle() + ": " + page.getDepth());
		for (Integer childId:page.getChildIds()) {

			PageDepthSummary child = new PageDepthSummary() ;
			child.setDepth(page.getDepth() + 1);
			child.setDepthForwarded(false);
			child.setChildIds(new ArrayList<Integer>());

			try {
				keyOut.datum(childId);
				valOut.datum(child);
				context.write(keyOut, valOut) ;
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		page.setDepthForwarded(true);
	}
	
}
