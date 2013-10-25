package hu.sztaki.ilab.cumulonimbus.util;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * @author klorand
 */
public class PactRecordHelper {
    public static double[] transpmultiply(final double[] a, final double[] b) {
        double[] ret = new double[a.length * b.length];
        for (int i = 0; i < a.length; ++i) {
            for (int j = 0; j < b.length; ++j) {
                ret[i * (a.length-1) + j] = a[i] * b[j];
            }
        }

        return ret;
    }

    public static double[] readFields(PactRecord record, int fromIndex, int count) {
       double[] ret = new double[count];
       for (int i=0; i<count; ++i){
           ret[i] = record.getField(fromIndex+i,PactDouble.class).getValue();
       }
       return ret;
    }

    public static void setFields(PactRecord outputRecord, int fromIndex, double[] qjqjt) {
        for (int i=0; i<qjqjt.length; ++i) {
            outputRecord.setField(fromIndex + i, new PactDouble(qjqjt[i]));
        }
    }
}
