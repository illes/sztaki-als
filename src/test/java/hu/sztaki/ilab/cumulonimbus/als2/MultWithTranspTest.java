package hu.sztaki.ilab.cumulonimbus.als2;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author klorand
 */
public class MultWithTranspTest {
    @Test
    public void testTransp() {
       double[] ret = MultWithTransp.transpmultiply(new double[]{1,2,3}, new double[]{1,2});
       assertThat(ret, is(new double[]{1, 2, 2, 4, 3, 6}));
    }



}
