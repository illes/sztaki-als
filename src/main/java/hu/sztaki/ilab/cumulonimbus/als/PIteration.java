package hu.sztaki.ilab.cumulonimbus.als;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import Jama.Matrix;
import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PIteration extends CoGroupStub {

  private static int k;
  private int id_;
  private final PactRecord result_ = new PactRecord();
  private Logger logger = null;
  
  @Override
  public void open(Configuration conf) {
    k = conf.getInteger(ALS.K, 1);
	  logger = ALS.getLogger(conf.getString("logFile", null));
  }
  
  @Override
  public void coGroup(Iterator<PactRecord> matrixElements, Iterator<PactRecord> q,
      Collector<PactRecord> out) {
	  if (logger != null) logger.info("Started P.coGroup()");
    double[][] matrix = new double[k][k];
    double[][] vector = new double[k][1];
    
    Map<Integer, Double> ratings = new HashMap<Integer, Double>();
    while (matrixElements.hasNext()) {
      PactRecord element = matrixElements.next();
      id_ = element.getField(0, PactInteger.class).getValue();
      ratings.put(element.getField(1, PactInteger.class).getValue(), 
          element.getField(2, PactDouble.class).getValue());
    }
    
    while (q.hasNext()) {
      PactRecord column = q.next();
      id_ = column.getField(0, PactInteger.class).getValue();
      double rating = ratings.get(column.getField(1, PactInteger.class).getValue());
      for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
          matrix[i][j] += column.getField(i + 2, PactDouble.class).getValue() * 
              column.getField(j + 2, PactDouble.class).getValue();
        }
        vector[i][0] += rating * column.getField(i + 2, PactDouble.class).getValue();
      }
    }
    
    Matrix a = new Matrix(matrix);
    Matrix b = new Matrix(vector);
    Matrix result = a.solve(b);
    
    for (int i = 0; i < k; ++i) {
      result_.setField(i + 1, new PactDouble(result.get(i, 0)));
    }
    result_.setField(0, new PactInteger(id_));
    out.collect(result_);
    if (logger != null) logger.info("Finished P.coGroup()");
  }
 
}
