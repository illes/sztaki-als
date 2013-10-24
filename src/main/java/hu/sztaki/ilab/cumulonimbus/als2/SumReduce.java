/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.cumulonimbus.als2;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import java.util.Iterator;

/**
 *
 * @author lukacsg
 */
public class SumReduce extends ReduceStub {
    private final PactRecord output = new PactRecord();
    private double sum;
    private PactRecord tmp;

    private int k;
    private int index;
    private final PactRecord output_ = new PactRecord();

    @Override
    public void open(Configuration parameters) throws Exception {
        k = parameters.getInteger(ALS.K, 1);
        index = parameters.getInteger(ALS.INDEX, 1);
    }

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
        tmp = records.next();
        output.setField(0, tmp.getField(0, PactInteger.class));
        sum = tmp.getField(3, null)
    }
    
}
