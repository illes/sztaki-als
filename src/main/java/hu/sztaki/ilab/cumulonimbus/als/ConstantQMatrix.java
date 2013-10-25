package hu.sztaki.ilab.cumulonimbus.als;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ConstantQMatrix extends ReduceStub {

  private final PactRecord vector = new PactRecord();
  private static final PactDouble ONE = new PactDouble(1.0);
  
  @Override
  public void reduce(Iterator<PactRecord> matrixElements, Collector<PactRecord> out)
      throws Exception {
    PactRecord element = matrixElements.next();
    vector.setField(0, element.getField(1, PactInteger.class));
    vector.setField(1, ONE);
    out.collect(vector);
  }

}
