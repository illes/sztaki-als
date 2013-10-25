package hu.sztaki.ilab.cumulonimbus.als2;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import hu.sztaki.ilab.cumulonimbus.util.PactRecordHelper;


/**
 * (j,qj) -> (j,qj*transp(qj))
 *
 * @author klorand
 */
public class MultWithTransp extends MapStub {

	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();


    @Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}


	@Override
    /**
     * (j,qj) -> (j,qj*transp(qj))
     */
	public void map(PactRecord record, Collector<PactRecord> collector) {
		PactInteger j = record.getField(0, PactInteger.class);
		double[] qj = PactRecordHelper.readFields(record, 1, record.getNumFields() - 1);

        double[] qjqjt = PactRecordHelper.transpmultiply(qj, qj);

        this.outputRecord.setField(0, j);
        PactRecordHelper.setFields(this.outputRecord, 1, qjqjt);
		collector.collect(this.outputRecord);
	}

}
