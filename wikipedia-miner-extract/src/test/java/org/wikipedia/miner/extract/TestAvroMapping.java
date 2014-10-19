package org.wikipedia.miner.extract;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.junit.Test;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;

public class TestAvroMapping {

	@Test
	public void testText2Avro() {
		String s = "!	{\"linkDocCount\": 0, \"linkOccCount\": 0, \"textDocCount\": 1, \"textOccCount\": 1}";
		
		Schema keySchema = Schema.create(Type.STRING);
		Schema valSchema = LabelOccurrences.getClassSchema();
		
		int i = s.indexOf('\t');
		String k = s.substring(0, i);
		String v = s.substring(i+1);
		
		AvroKey keyOut = new AvroKey();
		AvroValue valOut = new AvroValue();
		
		
		DatumReader<AvroKey> keyReader = new GenericDatumReader<AvroKey>(keySchema);
		DatumReader<AvroValue> valReader = new GenericDatumReader<AvroValue>(valSchema);
		
		try {
			JsonDecoder keyDecoder = DecoderFactory.get().jsonDecoder(keySchema, "\"" + k + "\"");
			keyOut.datum(keyReader.read(null, keyDecoder));
			System.out.println(keyOut);
			
			JsonDecoder valDecoder = DecoderFactory.get().jsonDecoder(valSchema, v);
			valOut.datum(valReader.read(null, valDecoder));
			System.out.println(valOut);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
