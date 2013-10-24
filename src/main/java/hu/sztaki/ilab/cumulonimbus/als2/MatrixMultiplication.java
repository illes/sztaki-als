/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.cumulonimbus.als2;

//import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 *
 * @author lukacsg
 */
public class MatrixMultiplication extends MatchStub {
    private final PactRecord output = new PactRecord();
    private double rating;
//    private int k;
//
//    @Override
//    public void open(Configuration conf) {
//        k = conf.getInteger(ALS.K, 1);
//    }
    
    @Override
    public void match(PactRecord q, PactRecord r, Collector<PactRecord> out) throws Exception {
        output.setField(0, r.getField(0, PactInteger.class));
        output.setField(1, r.getField(1, PactInteger.class));
        rating = r.getField(2, PactDouble.class).getValue();
        for(int i= 1;i < q.getNumFields(); ++i) {
            output.setField(i+1, new PactDouble(rating*q.getField(i, PactDouble.class).getValue()));
        }
        out.collect(output);
    }
    
}
