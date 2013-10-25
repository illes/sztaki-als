package hu.sztaki.ilab.cumulonimbus.als2;

import java.util.Iterator;
import java.util.Random;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PseudoRandomMatrix extends ReduceStub {

  private static int k;
  private final PactRecord vector = new PactRecord();
  private long seed = 42;
  
  @Override
  public void open(Configuration conf) {
	    k = conf.getInteger(ALS.K, 1);
	    seed = conf.getInteger("seed", 42);
  }

  @Override
  public void reduce(Iterator<PactRecord> elements, Collector<PactRecord> out)
      throws Exception {
    PactRecord element = elements.next();
    PactInteger _id = element.getField(1, PactInteger.class);
    vector.setField(0, _id);
    Random random = new Random(seed ^ _id.getValue());
    for (int i = 0; i < k; ++i) {
      vector.setField(i + 1, new PactDouble(random.nextDouble() - 0.5));
    }
    out.collect(vector);
  }
}
