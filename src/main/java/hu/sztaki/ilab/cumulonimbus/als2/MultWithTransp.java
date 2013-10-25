package hu.sztaki.ilab.cumulonimbus.als2;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;


/**
 * (j,qj) -> (j,qj*transp(qj))
 *
 * @author klorand
 */
public class MultWithTransp extends MapStub {

	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();


	static double[] transpmultiply(final double[] a, final double[] b) {
		double[] ret = new double[a.length * b.length];
		for (int i = 0; i < a.length; ++i) {
			for (int j = 0; j < b.length; ++j) {
				ret[i * (a.length-1) + j] = a[i] * b[j];
			}
		}

		return ret;
	}


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}


	@Override
    /**
     * (j,qj) -> (j,qj*transp(qj))
     */
	public void map(PactRecord record, Collector<PactRecord> collector) {
		PactInteger j = record.getField(0, PactInteger.class);
		double[] qj = readFields(record, 1, record.getNumFields() - 1);

        double[] qjqjt = transpmultiply(qj, qj);

        this.outputRecord.setField(0, j);
        setFields(this.outputRecord, 1, qjqjt);
		collector.collect(this.outputRecord);
	}

    static double[] readFields(PactRecord record, int fromIndex, int count) {
       double[] ret = new double[count];
       for (int i=0; i<count; ++i){
           ret[i] = record.getField(fromIndex+i,PactDouble.class).getValue();
       }
       return ret;
    }

    static void setFields(PactRecord outputRecord, int fromIndex, double[] qjqjt) {
        for (int i=0; i<qjqjt.length; ++i) {
            outputRecord.setField(fromIndex + i, new PactDouble(qjqjt[i]));
        }
    }
}
