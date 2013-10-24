package hu.sztaki.ilab.cumulonimbus.als;

import java.util.Iterator;

import Jama.Matrix;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class Iteration2 extends CoGroupStub {

  private static int k;
  private final PactRecord output_ = new PactRecord();
  
  @Override
  public void open(Configuration parameters) throws Exception {
    k = parameters.getInteger("k", 1);
  }
  
  @Override
  public void coGroup(Iterator<PactRecord> matrices, Iterator<PactRecord> columns,
      Collector<PactRecord> out) throws Exception {
    
    int id = 0;
    double[][] matrix = new double[k][k];
    double[][] column = new double[k][1];
    
    while (matrices.hasNext()) {
      PactRecord matrixRecord = matrices.next();
      id = matrixRecord.getField(0, PactInteger.class).getValue();
      for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
          matrix[i][j] += matrixRecord.getField(i * k + j + 1, PactDouble.class).getValue();
        }
      }
    }
    
    while (columns.hasNext()) {
      PactRecord columnRecord = columns.next();
      for (int i = 0; i < k; ++i) {
        column[i][0] += columnRecord.getField(i + 1, PactDouble.class).getValue();
      }
    }
    
    Matrix a = new Matrix(matrix);
    Matrix b = new Matrix(column);
    Matrix p = a.solve(b);
    
    output_.setField(0, new PactInteger(id));
    for (int i = 0; i < k; ++i) {
      output_.setField(i + 1, new PactDouble(p.get(i, 0)));
    }
    out.collect(output_);
  }

}
