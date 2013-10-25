package hu.sztaki.ilab.cumulonimbus.als2;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.util.Iterator;


/**
 * match (j, qj*transp(qij)) (i,j,rij) on val1.j = val2.getField(1-index)
 * emits (val2.getField(index), sumMat)
 *
 * @author klorand
 */
public class SumMat extends CoGroupStub {

	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();

    private int index;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
        index = parameters.getInteger(ALS.INDEX, 1);
	}

    @Override
    public void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector<PactRecord> out) throws Exception {

    }
}
