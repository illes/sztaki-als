package hu.sztaki.ilab.cumulonimbus.util;

import eu.stratosphere.pact.common.type.PactRecord;

public interface ByteReader {

  void start(PactRecord record, int index);
  void add(byte data);
  void finish();
}
