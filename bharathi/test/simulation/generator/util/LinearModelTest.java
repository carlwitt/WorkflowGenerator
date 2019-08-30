package simulation.generator.util;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Created by Carl Witt on 13.11.17.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
class LinearModelTest {

    @Test
    void randomModel(){
        LinearModel linearModel = LinearModel.randomMemoryModel(1000, 100e3, 24e9, 0.5, 0.5, 2.0);
        System.out.println("linearModel = " + linearModel);
        double[][] samples = linearModel.getSamples();
        SummaryStatistics sstats = new SummaryStatistics();
        for(Double mem: samples[1]){
            sstats.addValue(mem);
        }
        System.out.println("sstats.getMean() = " + sstats.getMean());
        System.out.println("sstats.getStandardDeviation() = " + sstats.getStandardDeviation());
//        System.out.println("linearModel.getSamples() = " + Arrays.deepToString(linearModel.getSamples()));
    }

    @Test
    void getPeakMemoryConsumption() {
        LinearModel linear = new LinearModel(2d, 10d, 20, 10e6);
        for (int i = 0; i < 10000; i++) {
            long first = linear.generate(i);
            long second = linear.generate(i);
            // assert deterministic error
            Assertions.assertEquals(first, second);
            // assert non-negativity
            Assertions.assertTrue(first >= 0);
        }
    }

}