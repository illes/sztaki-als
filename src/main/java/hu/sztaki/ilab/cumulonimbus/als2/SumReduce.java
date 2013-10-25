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
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author lukacsg
 */
public class SumReduce extends ReduceStub {
    private final PactRecord output = new PactRecord();
    private PactRecord tmp;
    private final List<Double> list = new ArrayList<Double>();
//    private int k;
    private int index;

    @Override
    public void open(Configuration parameters) throws Exception {
//        k = parameters.getInteger(ALS.K, 1);
        index = parameters.getInteger(ALS.INDEX, 1);
    }

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
        tmp = records.next();
        output.setField(0, tmp.getField(index, PactInteger.class));
        list.clear();
        for(int i= 2;i < tmp.getNumFields(); ++i) {
            list.add(i-2, tmp.getField(i, PactDouble.class).getValue());
        }
        while(records.hasNext()) {
            tmp = records.next();
            for (int i = 0; i < list.size(); ++i) {
                list.set(i, list.get(i) + tmp.getField(i + 2, PactDouble.class).getValue());
            }
        }
        for (int i = 0; i < list.size(); ++i) {
            output.setField(i+1, new PactDouble(list.get(i)));
        }
        out.collect(output);
    }
    
}
