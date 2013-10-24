package hu.sztaki.ilab.cumulonimbus.als;

import eu.stratosphere.pact.client.LocalExecutor;
import hu.sztaki.ilab.cumulonimbus.inputformat.MatrixElementInputFormat;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import com.sun.jndi.toolkit.url.Uri;

import eu.stratosphere.pact.common.contract.CoGroupContract;
// import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.generic.contract.Contract;
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

public class ALS implements PlanAssembler, PlanAssemblerDescription {

  @Override
  public Plan getPlan(String... args) {
    // parse job parameters
    int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String matrixInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
    int iteration = (args.length > 4 ? Integer.parseInt(args[4]) : 1);
    
    String logFile;
	try {
		logFile = new Uri(output + "/progress.log").getPath();
	} catch (MalformedURLException e) {
		logFile = null;
	}
    Logger logger = getLogger(logFile);
    logger.info("ALS.getPlan");
    
    FileDataSource matrixSource = new FileDataSource(
        MatrixElementInputFormat.class, matrixInput, "Input Matrix");
    DelimitedInputFormat.configureDelimitedFormat(matrixSource)
    .recordDelimiter('\n');

    Contract q = ReduceContract
        .builder(RandomMatrix.class, PactInteger.class, 1)
        .input(matrixSource)
        .name("Create q as a random matrix")
        .build();
    q.setParameter("k", k);
    q.setParameter("logFile", logFile);
    
    Contract p = null;
    
    for (int i = 0; i < iteration; ++i) {

      MatchContract multipliedQ = MatchContract
          .builder(MultiplyVector.class, PactInteger.class, 1, 0)
          .input1(matrixSource)
          .input2(q)
          .name("Sends the columns of q with multiple keys)")
          .build();
      
      p = CoGroupContract
          .builder(PIteration.class, PactInteger.class, 0, 0)
          .input1(matrixSource)
          .input2(multipliedQ)
          .name("For fixed q calculates optimal p")
          .build();
      p.setParameter("k", k);
      p.setParameter("logFile", logFile);
      
      MatchContract multipliedP = MatchContract
          .builder(MultiplyVector.class, PactInteger.class, 0, 0)
          .input1(matrixSource)
          .input2(p)
          .name("sends the rows of p with multiple keys")
          .build();

      q = CoGroupContract
          .builder(QIteration.class, PactInteger.class, 1, 1)
          .input1(matrixSource)
          .input2(multipliedP)
          .name("For fixed p calculates optimal q")
          .build();
      q.setParameter("k", k);
      q.setParameter("logFile", logFile);

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


  public static void main(String[] args) throws Exception {
        ALS als = new ALS();
        Plan toExecute = als.getPlan(args);
        LocalExecutor executor = new LocalExecutor();
        executor.start();
        long runtime = executor.executePlan(toExecute);
        System.out.println("runtime:  " + runtime);
        executor.stop();
  }
  
	public static Logger getLogger(String fileName) {
		if (fileName == null) return null;
		try {
			Logger logger = Logger.getLogger(ALS.class);
			// setting up a FileAppender dynamically...
			SimpleLayout layout = new SimpleLayout();
			FileAppender appender;
			appender = new FileAppender(layout, fileName, false);
			logger.addAppender(appender);

			logger.setLevel(Level.DEBUG);
			return logger;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
