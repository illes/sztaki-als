package hu.sztaki.ilab.cumulonimbus.inputformat;

import hu.sztaki.ilab.cumulonimbus.util.ByteReader;
import hu.sztaki.ilab.cumulonimbus.util.DoubleReaderNew;
import hu.sztaki.ilab.cumulonimbus.util.IntegerReaderFinal;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public class ColumnInputFormat extends DelimitedInputFormat {

  private static final long serialVersionUID = 3406020910594746582L;
  private final ByteReader rowReader_ = new IntegerReaderFinal();
  private final ByteReader elementReader_ = new DoubleReaderNew();
  private CsvReader csvReader = new CsvReader();

  @Override
  public boolean readRecord(PactRecord record, byte[] line, int offset, int numBytes) {
    csvReader.startLine(record, line, offset, numBytes);
    csvReader.read(rowReader_, '|');
    while (csvReader.hasMore()) {
      csvReader.read(elementReader_, '|');
    }
    return true;
  }

}
