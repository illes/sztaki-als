package hu.sztaki.ilab.cumulonimbus.als;

import hu.sztaki.ilab.cumulonimbus.inputformat.ColumnInputFormat;
import hu.sztaki.ilab.cumulonimbus.inputformat.MatrixElementInputFormat;

import java.util.ArrayList;
import java.util.Collection;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.contract.Contract;

public class ALS2 implements PlanAssembler, PlanAssemblerDescription {

  public static final String K = "k";
  public static final String INDEX = "index";
  
  @Override
  public Plan getPlan(String... args) {
    // parse job parameters
    int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String matrixInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
    int iteration = (args.length > 4 ? Integer.parseInt(args[4]) : 1);
    
    FileDataSource matrixSource = new FileDataSource(
        MatrixElementInputFormat.class, matrixInput, "Input Matrix");
    DelimitedInputFormat.configureDelimitedFormat(matrixSource)
    .recordDelimiter('\n');

    //for reading q from file
    String qInput = (args.length > 5 ? args[5] : "");
    FileDataSource qSource = 
        new FileDataSource(ColumnInputFormat.class, qInput, "Input Q");
    DelimitedInputFormat.configureDelimitedFormat(qSource).recordDelimiter('\n');
    
    Contract q = (Contract) qSource;

    //for creating the constant 1 matrix
    //only works for k = 1
/*    Contract q = ReduceContract
        .builder(ConstantMatrix.class, PactInteger.class, 1)
        .input(matrixSource)
        .name("Create q as a constant 1 matrix")
        .build();*/

    /*
    //for creating a random matrix
    Contract q = ReduceContract
        .builder(RandomMatrix.class, PactInteger.class, 1)
        .input(matrixSource)
        .name("Create q as a random matrix")
        .build();
    q.setParameter(K, k);
    */
    Contract p = null;
    
    for (int i = 0; i < iteration; ++i) {

      MatchContract matrix = MatchContract
          .builder(MatrixPreCalc.class, PactInteger.class, 1, 0)
          .input1(matrixSource)
          .input2(q)
          .name("MatrixPreCalc")
          .build();
      matrix.setParameter(K, k);
      matrix.setParameter(INDEX, 0);
      
      MatchContract vector = MatchContract
          .builder(VectorPreCalc.class, PactInteger.class, 1, 0)
          .input1(matrixSource)
          .input2(q)
          .name("VectorPreCalc")
          .build();
      vector.setParameter(K, k);
      vector.setParameter(INDEX, 0);
      
      p = CoGroupContract
          .builder(Iteration2.class, PactInteger.class, 0, 0)
          .input1(matrix)
          .input2(vector)
          .name("For fixed q calculates optimal p")
          .build();
      p.setParameter(K, k);
      
      matrix = MatchContract
          .builder(MatrixPreCalc.class, PactInteger.class, 0, 0)
          .input1(matrixSource)
          .input2(p)
          .name("MatrixPreCalc")
          .build();
      matrix.setParameter(K, k);
      matrix.setParameter(INDEX, 1);
      
      vector = MatchContract
          .builder(VectorPreCalc.class, PactInteger.class, 0, 0)
          .input1(matrixSource)
          .input2(p)
          .name("VectorPreCalc")
          .build();
      vector.setParameter(K, k);
      vector.setParameter(INDEX, 1);
      
      q = CoGroupContract
          .builder(Iteration2.class, PactInteger.class, 0, 0)
          .input1(matrix)
          .input2(vector)
          .name("For fixed p calculates optimal q")
          .build();
      q.setParameter(K, k);
      
    }

    FileDataSink pOut = new FileDataSink(RecordOutputFormat.class, output + "/p",
        p, "ALS P output");
    RecordOutputFormat.configureRecordFormat(pOut).recordDelimiter('\n')
    .fieldDelimiter(' ').lenient(true).field(PactInteger.class, 0);
   
    for (int i = 0; i < k; ++i) {
      RecordOutputFormat.configureRecordFormat(pOut).field(PactDouble.class, i + 1);
    }
    
    FileDataSink qOut = new FileDataSink(RecordOutputFormat.class, output + "/q",
        q, "ALS Q output");
    RecordOutputFormat.configureRecordFormat(qOut).recordDelimiter('\n')
    .fieldDelimiter(' ').lenient(true).field(PactInteger.class, 0);
   
    for (int i = 0; i < k; ++i) {
      RecordOutputFormat.configureRecordFormat(qOut).field(PactDouble.class, i + 1);
    }
     
    Collection<GenericDataSink> outputs = new ArrayList<GenericDataSink>();
    outputs.add(pOut);
    outputs.add(qOut);
    
    Plan plan = new Plan(outputs, "ALS");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }


  @Override
  public String getDescription() {
    return "Parameters: [noSubStasks] [matrix] [output] [rank] [numberOfIterations]";
  }


}
