package hu.sztaki.ilab.cumulonimbus.inputformat;

import hu.sztaki.ilab.cumulonimbus.util.ByteReader;
import hu.sztaki.ilab.cumulonimbus.util.DoubleReaderFinal;
import hu.sztaki.ilab.cumulonimbus.util.IntegerReaderFinal;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public class MatrixElementInputFormat extends DelimitedInputFormat {
  
  private static final long serialVersionUID = -1945843964517850356L;
  private final ByteReader rowReader_ = new IntegerReaderFinal();
  private final ByteReader columnReader_ = new IntegerReaderFinal();
  private final ByteReader valueReader_ = new DoubleReaderFinal();
  private CsvReader csvReader = new CsvReader();

  @Override
  public boolean readRecord(PactRecord record, byte[] line, int offset,
      int numBytes) {
    csvReader.startLine(record, line, offset, numBytes);
    csvReader.read(rowReader_, '|');
    csvReader.read(columnReader_, '|');
    csvReader.read(valueReader_, '|');
    return true;
  }
}
