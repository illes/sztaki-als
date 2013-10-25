package hu.sztaki.ilab.cumulonimbus.als2;

import hu.sztaki.ilab.cumulonimbus.util.PactRecordHelper;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author klorand
 */
public class MultWithTranspTest {
    @Test
    public void testTransp() {
       double[] ret = PactRecordHelper.transpmultiply(new double[]{1, 2, 3}, new double[]{1, 2});
       assertThat(ret, is(new double[]{1, 2, 2, 4, 3, 6}));
    }



}
