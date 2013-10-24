package hu.sztaki.ilab.cumulonimbus.als;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class VectorPreCalc extends MatchStub {

  private static int k;
  private int index;
  private final PactRecord output_ = new PactRecord();
  
  @Override
  public void open(Configuration parameters) throws Exception {
    k = parameters.getInteger(ALS2.K, 1);
    index = parameters.getInteger(ALS2.INDEX, 1);
  }
  
  @Override
  public void match(PactRecord matrix, PactRecord column, Collector<PactRecord> out)
      throws Exception {
    output_.setField(0, matrix.getField(index, PactInteger.class));
    double mtx = matrix.getField(2, PactDouble.class).getValue();
    for (int i = 0; i < k; ++i) {
      output_.setField(i + 1, new PactDouble(
          mtx * column.getField(i + 1, PactDouble.class).getValue()));
    }
    out.collect(output_);
  }

}
