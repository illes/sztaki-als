package hu.sztaki.ilab.cumulonimbus.util;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class IntegerReaderFinal implements ByteReader {

  private int value = 0;
  private PactRecord record_;
  private int index_;
  private final PactInteger result_ = new PactInteger();
  private boolean positive = true;
  
  @Override
  public void start(PactRecord record, int index) {
    value = 0;
    record_ = record;
    index_ = index;
  }

  @Override
  public void add(byte data) {
    if (data == '-') {
      positive = false;
    } else {
      value *= 10;
      value += data - '0';
    }
  }

  @Override
  public void finish() {
    result_.setValue(positive ? value : -value);
    record_.setField(index_, result_);
  }
  
}
