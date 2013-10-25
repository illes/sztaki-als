package hu.sztaki.ilab.cumulonimbus.als;

import Jama.Matrix;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class QIteration extends CoGroupStub {

  private static int k;
  private int id_;
  private final PactRecord result_ = new PactRecord();
  private static Logger logger = Logger.getLogger(QIteration.class);
  
  @Override
  public void open(Configuration conf) {
    k = conf.getInteger(ALS.K, 1);
    if (logger != null) logger.info("Opening Q");
  }
  
  @Override
  public void coGroup(Iterator<PactRecord> matrixElements, Iterator<PactRecord> p,
      Collector<PactRecord> out) {
    double[][] matrix = new double[k][k];
    double[][] vector = new double[k][1];
    double yTy = 0;
    
    Map<Integer, Double> ratings = new HashMap<Integer, Double>();
    while (matrixElements.hasNext()) {
      PactRecord element = matrixElements.next();
      id_ = element.getField(1, PactInteger.class).getValue();
      ratings.put(element.getField(0, PactInteger.class).getValue(), 
          element.getField(2, PactDouble.class).getValue());
    }
    
    while (p.hasNext()) {
      PactRecord row = p.next();
      id_ = row.getField(1, PactInteger.class).getValue();
      double rating = ratings.get(row.getField(0, PactInteger.class).getValue());
      for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
          matrix[i][j] += row.getField(i + 2, PactDouble.class).getValue() * 
              row.getField(j + 2, PactDouble.class).getValue();
        }
        vector[i][0] += rating * row.getField(i + 2, PactDouble.class).getValue();
      }
      
      for (int i = 0; i < k; i++) yTy += rating * rating;
    }

    // poor man's regularization
    for (int i = 0; i < k; ++i) matrix[i][i] += hu.sztaki.ilab.cumulonimbus.als2.ALS.LAMBDA;
    
    Matrix a = new Matrix(matrix); // X^T * x + lambda * E
    Matrix b = new Matrix(vector); // X^T * y
    Matrix result = a.solve(b);
    
//  double errorSquare = result.transpose().times(a.times(result)).get(0, 0) - 2* result.transpose().times(b).get(0, 0) + yTy; // wT * A * w  - 2 * wT * b + yT*y
    
    result_.setField(0, new PactInteger(id_));
//  result_.setField(1, new PactDouble(errorSquare));
    for (int i = 0; i < k; ++i) {
      result_.setField(i + 1, new PactDouble(result.get(i, 0)));
    }

    out.collect(result_);
  }
  
}
