package hu.sztaki.ilab.cumulonimbus.inputformat;

import hu.sztaki.ilab.cumulonimbus.util.ByteReader;
import eu.stratosphere.pact.common.type.PactRecord;

public class CsvReader {

    private PactRecord record_;
    private byte[] line_;
    private int pos_;
    private int actualIndex_;
    private int limit_;

    public void startLine(PactRecord record, byte[] line, int offset,
            int numBytes) {
        record_ = record;
        line_ = line;
        pos_ = offset;
        actualIndex_ = 0;
        limit_ = offset + numBytes;
    }

    public void read(ByteReader byteReader, char separator) {
        byteReader.start(record_, actualIndex_);
        while (line_[pos_] != separator) {
            byteReader.add(line_[pos_]);
            ++pos_;
        }
        byteReader.finish();
        ++pos_;
        ++actualIndex_;
    }

    public boolean hasMore() {
        return pos_ < limit_;
    }

    public String getLine() {
        return new String(line_).substring(actualIndex_, limit_ - actualIndex_);
    }
}
