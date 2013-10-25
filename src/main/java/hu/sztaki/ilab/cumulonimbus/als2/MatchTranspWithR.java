/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.cumulonimbus.als2;

//import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 *
 * @author klorand
 */
public class MatchTranspWithR extends MatchStub {

	private final PactRecord output = new PactRecord();



	@Override
	public void match(PactRecord mat, PactRecord r, Collector<PactRecord> out) throws Exception {
		output.setField(0, r.getField(0, PactInteger.class));
		output.setField(1, r.getField(1, PactInteger.class));
		MultWithTransp.setFields(output, 2, MultWithTransp.readFields(mat,1, mat.getNumFields()-1));
		out.collect(output);
	}

}
