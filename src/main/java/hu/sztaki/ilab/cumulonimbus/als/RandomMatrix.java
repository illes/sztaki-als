package hu.sztaki.ilab.cumulonimbus.als;

import java.util.Iterator;
import java.util.Random;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class RandomMatrix extends ReduceStub {

  private static int k;
  private final PactRecord vector_ = new PactRecord();
  private final Random RANDOM = new Random();
  
  @Override
  public void open(Configuration conf) {
    k = conf.getInteger("k", 1);
  }

  @Override
  public void reduce(Iterator<PactRecord> elements, Collector<PactRecord> out)
      throws Exception {
    PactRecord element = elements.next();
    vector_.setField(0, element.getField(0, PactInteger.class));
    for (int i = 0; i < k; ++i) {
      vector_.setField(i + 1, new PactDouble(1 + RANDOM.nextDouble() / 2));
    }
    out.collect(vector_);
  }
}
