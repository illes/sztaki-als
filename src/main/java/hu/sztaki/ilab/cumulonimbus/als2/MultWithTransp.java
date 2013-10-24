package hu.sztaki.ilab.cumulonimbus.als2;

import java.util.ArrayList;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;


/**
 * @author klorand
 */
public class MultWithTransp extends MapStub {

	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();
	private PactString emittedKey;
    private int k;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		emittedKey = new PactString(parameters.getString("emittedKey", "SUM-QQT"));
        k = parameters.getInteger(ALS.K, 1);
	}


	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {

		PactList<PactDouble> qi = record.getField(1, PactList.class);

		PactList<PactList<PactDouble>> qiqit = transpmultiply(qi, qi);

		// we emit a (word, 1) pair
		this.outputRecord.setField(0, emittedKey);
		this.outputRecord.setField(1, qiqit);
		collector.collect(this.outputRecord);
	}


	private PactList<PactList<PactDouble>> transpmultiply(final PactList<PactDouble> qi, final PactList<PactDouble> qi1) {
		final PactList<PactList<PactDouble>> retList = new PactList<PactList<PactDouble>>(new ArrayList<PactList<PactDouble>>()) {};
        for (int i=0; i<qi.size(); ++i) {
           for (int j=0; j<qi1.size(); ++j) {

           }
        }

		return retList;
	}
}
