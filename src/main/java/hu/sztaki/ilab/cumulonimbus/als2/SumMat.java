package hu.sztaki.ilab.cumulonimbus.als2;

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;


/**
 * reduce (i, j, qj*transp(qij)) on 1-index
 * emits (val2.getField(index), sumMat)
 *
 * @author klorand
 */
public class SumMat extends ReduceStub {

	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();
	private int index;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		index = parameters.getInteger(ALS.INDEX, 1);
	}


	@Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
       PactInteger emmittedKey = null;
       double[ ] summed = null;
       while (records.hasNext()) {
           PactRecord rec = records.next();
           if (emmittedKey==null) {
               emmittedKey = rec.getField(index, PactInteger.class);
           }
           if (summed==null) {
               summed = new double[rec.getNumFields()-2];
           }
           for (int i=0; i<summed.length; ++i) {
               summed[i] += rec.getField(2+i, PactDouble.class).getValue();
           }
       }
       outputRecord.setField(0, emmittedKey);
       MultWithTransp.setFields(outputRecord, 1, summed);
    }
}
