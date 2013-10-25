package hu.sztaki.ilab.cumulonimbus.als2;

import java.util.ArrayList;
import java.util.Collection;

import hu.sztaki.ilab.cumulonimbus.als.ConstantPMatrix;
import hu.sztaki.ilab.cumulonimbus.als.ConstantQMatrix;
import hu.sztaki.ilab.cumulonimbus.als.MultiplyVector;
import hu.sztaki.ilab.cumulonimbus.als.PIteration;
import hu.sztaki.ilab.cumulonimbus.als.QIteration;
import hu.sztaki.ilab.cumulonimbus.inputformat.MatrixElementInputFormat;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
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

public class UberALS implements PlanAssembler, PlanAssemblerDescription {
	
	public static double LAMBDA = 0;

	public static final String K = "k";
	public static final String INDEX = "index";

	public static final Logger logger = Logger.getLogger("ALS");

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String matrixInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");
		int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
		int iteration = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		logger.info("ALS.getPlan");

		FileDataSource matrixSource = new FileDataSource(MatrixElementInputFormat.class, matrixInput, "Input Matrix");
		DelimitedInputFormat.configureDelimitedFormat(matrixSource).recordDelimiter('\n');

		// for reading q from file
		int qSeed = (args.length > 5 ? Integer.parseInt(args[5]) : 42);

//		 Contract q = (Contract) qSource;
		Collection<GenericDataSink> outputs = new ArrayList<GenericDataSink>();

		Contract p;
		Contract q;
		if (true) {
			logger.warn("for creating the constant 1 matrix, only works for k = 1");
			if (k != 1)
				throw new IllegalArgumentException("only works for k = 1");
			q = ReduceContract.builder(ConstantQMatrix.class, PactInteger.class, 1).input(matrixSource).name("Create q as a constant 1 matrix").build();
			p = ReduceContract.builder(ConstantPMatrix.class, PactInteger.class, 0).input(matrixSource).name("Create p as a constant 1 matrix").build();
			outputs.add(createFileDataSink(output + "/p.0", p, "ALS P output init", k, false));
			outputs.add(createFileDataSink(output + "/q.0", q, "ALS Q output init", k, false));
		} else {
			logger.info("creating a pseudo random matrix");
			// for creating a random matrix
			q = ReduceContract.builder(PseudoRandomMatrix.class, PactInteger.class, 1 /* i */).input(matrixSource).name("Create q as a random matrix").build();
			q.setParameter(K, k);
			q.setParameter("seed", qSeed);
			/* i, f_random */
			p = null; // will be computed
		}

		for (int i = 0; i < iteration; ++i) {

			MatchContract multiplicationRQ = MatchContract.builder(MatrixMultiplication.class, PactInteger.class, 0 /*j*/, 1 /*j*/)
                                .input1(q)
                                .input2(matrixSource)
				.name("Make the multiplication with q "+i).build();

			ReduceContract rowsum = ReduceContract.builder(SumReduce.class, PactInteger.class, 0)
                                .input(multiplicationRQ)
                                .name("Sum the rows. "+i).build();
			rowsum.setParameter(INDEX, 0);
                        
                        ///////// q*q
                        MapContract qq = MapContract.builder(MultWithTransp.class)
                                .input(q)
                                .name("q*q " + i).build();
                        
                        MatchContract matchrq = MatchContract.builder(MatchTranspWithR.class , PactInteger.class, 0, 1)
                                .input1(qq)
                                .input2(matrixSource)
                                .name("Match R q " + i).build();
                        
                        ReduceContract summatq = ReduceContract.builder(SumMat.class, PactInteger.class, 0)
                                .input(matchrq)
                                .name("Summat q "+i).build();
                        summatq.setParameter(INDEX, 1);
                        
                        p = MatchContract.builder(MatrixVectorMultiplication.class, PactInteger.class, 0, 0)
                                .input1(summatq)
                                .input2(rowsum)
                                .name("Make p "+i).build();
                        p.setParameter(K, k);

			outputs.add(createFileDataSink(output + "/p." + (i+1), p, "ALS P output " + i, k, false));

			MatchContract multiplicationRP = MatchContract.builder(MatrixMultiplication.class, PactInteger.class, 0 /*j*/, 0 /*j*/)
                                .input1(p)
                                .input2(matrixSource)
				.name("Make the multiplication with p "+i).build();

			ReduceContract columsum = ReduceContract.builder(SumReduce.class, PactInteger.class, 1)
                                .input(multiplicationRP)
                                .name("Sum the colums. "+i).build();
			columsum.setParameter(INDEX, 1);
                        
                        ///////// p*p
                        MapContract pp = MapContract.builder(MultWithTransp.class)
                                .input(p)
                                .name("p*p "+i).build();
                        
                        MatchContract matchrp = MatchContract.builder(MatchTranspWithR.class , PactInteger.class, 0, 0)
                                .input1(pp)
                                .input2(matrixSource)
                                .name("Match R p "+i).build();
                        
                        ReduceContract summatp = ReduceContract.builder(SumMat.class, PactInteger.class, 1)
                                .input(matchrp)
                                .name("Summat p "+i).build();
                        summatp.setParameter(INDEX, 0);
                        
                        q = MatchContract.builder(MatrixVectorMultiplication.class, PactInteger.class, 0, 0)
                                .input1(summatp)
                                .input2(columsum)
                                .name("Make p "+i).build();
                        q.setParameter(K, k);

			outputs.add(createFileDataSink(output + "/q." + (i+1), q, "ALS Q output " + i, k, false));
		}

		outputs.add(createFileDataSink(output + "/p", p, "ALS P output", k, false));
		outputs.add(createFileDataSink(output + "/q", q, "ALS Q output", k, false));

		Plan plan = new Plan(outputs, "ALS");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [matrix] [output] [rank] [numberOfIterations] [seed]";
	}

	public static void main(String[] args) throws Exception {
		UberALS als = new UberALS();
		Plan toExecute = als.getPlan(args);
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		long runtime = executor.executePlan(toExecute);
		System.out.println("runtime:  " + runtime);
		executor.stop();
	}

	private static FileDataSink createFileDataSink(String filePath, Contract input, String name, int k, boolean omitId) {
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, filePath, input, name);
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n').fieldDelimiter(' ').lenient(true);
		if (!omitId) RecordOutputFormat.configureRecordFormat(out).field(PactInteger.class, 0);

		for (int j = 0; j < k; ++j) {
			RecordOutputFormat.configureRecordFormat(out).field(PactDouble.class, j + 1);
		}
		return out;
	}
}
