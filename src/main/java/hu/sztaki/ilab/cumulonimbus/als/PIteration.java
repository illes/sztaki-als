package hu.sztaki.ilab.cumulonimbus.als;

import Jama.Matrix;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PIteration extends CoGroupStub {

  private static int k;
  private final PactRecord result_ = new PactRecord();
  private static Logger logger = Logger.getLogger(PIteration.class);
  
  @Override
  public void open(Configuration conf) {
    k = conf.getInteger(ALS.K, 1);
    if (logger != null) logger.info("Opening P");
  }
  
  @Override
  public void coGroup(Iterator<PactRecord> matrixElements, Iterator<PactRecord> q,
      Collector<PactRecord> out) {
    double[][] matrix = new double[k][k]; // A = X^T * X + lamba * E
    double[][] vector = new double[k][1]; // b = X^T * y

    int id_ = -1;
    TreeMap<Integer, Double> ratings = new TreeMap<Integer, Double>(); //itemid -> double
    while (matrixElements.hasNext()) {
      PactRecord element = matrixElements.next();
      id_ = element.getField(0, PactInteger.class).getValue();
      ratings.put(element.getField(1, PactInteger.class).getValue(), 
          element.getField(2, PactDouble.class).getValue());
    }
    if (id_ < 0)
    	throw new IllegalStateException();
    
    double _sumRatings = 0;
    while (q.hasNext()) {
      PactRecord column = q.next();
      //id_ = column.getField(0, PactInteger.class).getValue();
      double rating = ratings.get(column.getField(1, PactInteger.class).getValue());
      _sumRatings += rating;
      for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
          matrix[i][j] += column.getField(i + 2, PactDouble.class).getValue() * 
              column.getField(j + 2, PactDouble.class).getValue();
        }
        vector[i][0] += rating * column.getField(i + 2, PactDouble.class).getValue();
      }
    }
    if (id_ == 0) {
    	logger.warn("Got ratings sum: " + _sumRatings + " count: " + ratings.size());
    	logger.warn(ratings.values());
    }

    // poor man's regularization
    for (int i = 0; i < k; ++i) matrix[i][i] += hu.sztaki.ilab.cumulonimbus.als2.ALS.LAMBDA;
    
    Matrix a = new Matrix(matrix); // X^T * x + lambda * E
    Matrix b = new Matrix(vector); // X^T * y
    Matrix result = a.solve(b);

    if (id_ == 0) {
	    logger.warn("A:\t" + a.toString() + " " + matrix[0][0]);
	    logger.warn("B:\t" + b.toString() + " " + vector[0][0]);
	    logger.warn("result:\t" + result.toString() + " " + result.get(0, 0));
    }
    
//  double errorSquare = result.transpose().times(a.times(result)).get(0, 0) - 2* result.transpose().times(b).get(0, 0) + yTy; // wT * A * w  - 2 * wT * b + yT*y
    
    result_.setField(0, new PactInteger(id_));
//  result_.setField(1, new PactDouble(errorSquare));
    for (int i = 0; i < k; ++i) {
      result_.setField(i + 1, new PactDouble(result.get(i, 0)));
    }

    out.collect(result_);
  }
 
}
