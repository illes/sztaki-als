package hu.sztaki.ilab.cumulonimbus.util;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class DoubleReaderFinal implements ByteReader {
  
  private int intValue = 0;
  private int fracValue = 0;
  private int fracChars = 0;
  private PactRecord record_;
  private int index_;
  private final PactDouble result_ = new PactDouble();
  private boolean positive = true;

  @Override
  public void start(PactRecord record, int index) {
    positive = true;
    intValue = 0;
    fracValue = 0;
    fracChars = 0;
    record_ = record;
    index_ = index;
  }
  
  @Override
  public void add(byte data) {
    if (data == '-') {
      positive = false;
    } else if (data == '.') {
      fracChars = 1;
    } else {
      if (fracChars == 0) {
        intValue *= 10;
        intValue += data - '0';
      } else {
        fracValue *= 10;
        fracValue += data - '0';
        fracChars++;
      }
    }
  }
  
  @Override
  public void finish() {
    double value = intValue + (fracValue) * Math.pow(10, (-1 * (fracChars - 1)));
    result_.setValue(positive ? value : -value);
    record_.setField(index_, result_);
  }
}
