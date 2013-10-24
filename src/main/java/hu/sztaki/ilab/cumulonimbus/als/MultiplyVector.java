package hu.sztaki.ilab.cumulonimbus.als;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class MultiplyVector extends MatchStub {

  @Override
  public void match(PactRecord matrixElement, PactRecord columnOfQ, 
      Collector<PactRecord> out)
      throws Exception {
    PactRecord output = new PactRecord();
    output.setField(0, matrixElement.getField(0, PactInteger.class));
    output.setField(1, matrixElement.getField(1, PactInteger.class));
    for (int i = 0; i < columnOfQ.getNumFields() - 1; ++i) {
      output.setField(i + 2, columnOfQ.getField(i + 1, PactDouble.class));
    }
    out.collect(output);
  }

}
