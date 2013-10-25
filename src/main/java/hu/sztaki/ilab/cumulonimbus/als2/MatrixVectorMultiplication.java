/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.cumulonimbus.als2;

import Jama.CholeskyDecomposition;
import Jama.Matrix;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author lukacsg
 */
public class MatrixVectorMultiplication extends MatchStub {
    private final PactRecord output = new PactRecord();
    private final List<Double> list = new ArrayList<Double>();
    private int k;

    @Override
    public void open(Configuration conf) {
        k = conf.getInteger(ALS.K, 1);
    }
    
    @Override
    public void match(PactRecord matrixin, PactRecord vectorin, Collector<PactRecord> out) throws Exception {
        double[][] matrix = new double[k][k];
        double[][] vector = new double[k][1];
        output.setField(0, vectorin.getField(0, PactInteger.class));
        for (int i = 0; i < k; ++i) {
            for (int j = 0; j < k; ++j) {
                matrix[i][j] = (i != j) ? matrixin.getField(i*k+j + 1, PactDouble.class).getValue()
                        : matrixin.getField(i*k+j + 1, PactDouble.class).getValue() + 1e-6; // poor man's regularization
            }
            vector[i][0] = vectorin.getField(i + 1, PactDouble.class).getValue();
        }
        Matrix a = new Matrix(matrix); // X^T * x + lambda * E
        Matrix b = new Matrix(vector); // X^T * y
        Matrix result = new CholeskyDecomposition(a).solve(b);

        for (int i = 0; i < k; ++i) {
            output.setField(i + 1, new PactDouble(result.get(i, 0)));
        }

        out.collect(output);
    }
    
}
