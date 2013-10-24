package hu.sztaki.ilab.cumulonimbus.als;

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

public class PIteration extends CoGroupStub {

  private static int k;
  private int id_;
  private final PactRecord result_ = new PactRecord();
  private Logger logger = null;
  
  @Override
  public void open(Configuration conf) {
    k = conf.getInteger("k", 1);
	  logger = ALS.getLogger(conf.getString("logFile", null));
  }
  
  @Override
  public void coGroup(Iterator<PactRecord> matrixElements, Iterator<PactRecord> q,
      Collector<PactRecord> out) {
	  if (logger != null) logger.info("Started P.coGroup()");
    double[][] matrix = new double[k][k];
    double[] vector = new double[k];
    
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
        vector[i] += rating * column.getField(i + 2, PactDouble.class).getValue();
      }
    }
    
    double[][] inverse = inverseMatrix(matrix);
    double[] result = new double[k];
    
    for (int i = 0; i < k; ++i) {
      for (int j = 0; j < k; ++j) {
        result[i] += inverse[i][j] * vector[j];
      }
    }
    
    for (int i = 0; i < k; ++i) {
      result_.setField(i + 1, new PactDouble(result[i]));
    }
    result_.setField(0, new PactInteger(id_));
    out.collect(result_);
    if (logger != null) logger.info("Finished P.coGroup()");
  }
  
  
  
  private double[][] inverseMatrix(double[][] in){
    double[][] result = new double[k][k];
    double[][] old = new double[k][k * 2];
    double[][] newer = new double[k][k * 2];

    
    for (int v = 0; v < k; v++){//ones vector
      for (int s = 0; s < k * 2; s++){
        if (s - v == k) 
          old[v][s] = 1;
        if(s < k)
          old[v][s] = in[v][s];
      }
    }
    //zeros below the diagonal
    for (int v = 0; v < k; v++){
      for (int v1 = 0; v1 < k; v1++){
        for (int s = 0; s < k * 2; s++){
          if (v == v1)
            newer[v][s] = old[v][s] / old[v][v];
          else
            newer[v1][s] = old[v1][s];
        }
      }
      old = copy(newer);   
      for (int v1 = v+1; v1 < k; v1++){
        for (int s = 0; s < k * 2; s++){
          newer[v1][s] = old[v1][s] - old[v][s] * old[v1][v];
        }
      }
      old = copy(newer);
    }
    //zeros above the diagonal
    for (int s = k - 1; s > 0; s--){
      for (int v = s - 1; v >= 0; v--){
        for (int s1 = 0; s1 < k * 2; s1++){
          newer[v][s1] = old[v][s1] - old[s][s1] * old[v][s];
        }
      }
      old = copy(newer);
    }
    for (int v = 0; v < k; v++){//right part of matrix is inverse
      for (int s = k; s < k * 2; s++){
        result[v][s - k] = newer[v][s];
      }
    }
    return result;
  }

  private double[][] copy(double[][] in){
    double[][] out = new double[in.length][in[0].length];
    for(int v = 0; v < in.length; v++){
      for (int s = 0; s < in[0].length; s++){
        out[v][s] = in[v][s];
      }
    }
    return out;
  }

}
