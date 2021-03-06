/*
* ******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 *  tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
 * Hannes Schuh (HU Berlin)
 * Carl Witt (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * Jörgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universität zu Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package simulation.generator.util;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.DoubleStream;

/**
 * A class for sampling random numbers that potentially depend on an input size.
 * This is used to model the relationship between a task's read amount of data (sum of input file sizes) and
 * its peak memory consumption. We start from a linear model on input size and add unexplained variance.
 *
 * peak mem = max(min value, slope * input size + intercept + random value sampled from Normal(0, variance))
 *
 * Created by Carl Witt on 13.11.17.
 */
public class LinearModel extends MemoryModel {

    /** The slope of the linear function. */
    private final double slope;
    /** The axis intercept of the linear function. */
    private final double intercept;
    /** The random number generator used to generate the unexplained variation of the peak memory consumption (as opposed to the explained variation by the input file size). */
    private static Random error = new Random(129831239L);
    /** The standard deviation of the error that is added to the linear model.*/
    private double errorStandardDeviation;
    /** The smallest number ever returned by this model, for instance to assure drawing positive random numbers. */
    private double minValue = Double.MIN_VALUE;

    private static Random random = new Random();

    /**
     * peak mem will be sampled from slope * input size + intercept + random value in range [-err, +err]
     * @param slope The slope of the linear function.
     * @param intercept The axis intercept of the linear function.
     * @param errorStandardDeviation The standard deviation of the (zero mean) error added to the linear model.
     * @param minValue when randomly generating a smaller value than minValue, minValue is returned instead
     */
    public LinearModel(double slope, double intercept, double errorStandardDeviation, double minValue) {
        this.slope = slope;
        this.intercept = intercept;
        this.errorStandardDeviation = errorStandardDeviation;
        this.minValue = minValue;
    }

    /**
     * peak mem = slope * input size + intercept + random value sampled from Normal(0, errorStandardDeviation^2)
     * @param inputFileSize The size of the input file, usually in bytes.
     * @return a random value ≥ {@link #minValue} according to the model.
     */
    @Override
    public long generate(long inputFileSize){
        return (long) Math.max(minValue, inputFileSize*slope + intercept + error.nextGaussian()*errorStandardDeviation);
    }


    /** This was used to generate the random memory models for each task type in Witt et al. 2018.
     * Initializes the {@link #samples} array. */
    public static LinearModel randomMemoryModel(int numSamples, double minFileSize, double maxMemConsumption, double linearTaskChance, double minSlope, double maxSlope){

        // average memory usage between 1GB and 1TB
        double meanY = uniform(1e9, 500e9);
//        System.out.println("meanY = " + meanY);
        // standard deviation between 3% and 10% of the mean (seems small, but produces realistic feeling models; otherwise we get very large memory ranges)
        double varY = Math.pow(meanY * uniform(0.1, 0.5), 2.0);

        // zero slope in half of the cases, minSlope and maxSlope otherwise
        double slope = Math.random() > linearTaskChance ? 0. : uniform(minSlope, maxSlope);
        double intercept;
        double errorStandardDeviation;

        // the input sizes
        DoubleStream sampleX;

        if (slope < 1e-6) {
            // input range orientation between 100MB and 1GB (actual input sizes vary beyond these limits), but
            // this brings the input sizes into a good range
            double minInput = 100e6;
            double maxInput = 200e6;

            // since the input size has no effect on the output, it can be anything
            intercept = meanY;
            // all of the variance is caused by errors (i.e., is unexplained, since input size is the only explanatory factor we consider)
            errorStandardDeviation = Math.sqrt(varY);

            sampleX = gaussian((minInput+maxInput)/2.0, (maxInput-minInput)/3.0);

        } else {
            intercept = 0;

            // the linearity determines the amount of output variable variance explained by the input (is related but not the same as correlation)
            double linearity = uniform(0.25, 0.75);

            double meanX = (meanY-intercept) / slope ; // since E[mX+n] = m*E[X] + n

            // the variance of the input distribution depends on the variance of the output (more specifically, the variance explained by input)
            double varX = linearity * varY / Math.pow(slope, 2.0); // since Var[mX] = m^2 Var[X]

            errorStandardDeviation = Math.sqrt((1.-linearity) * varY);

            sampleX = gaussian(meanX, Math.sqrt(varX));

        }

        // construct linear model
        // set parameters
        LinearModel linearModel = new LinearModel(slope, intercept, errorStandardDeviation, 30e6);

        // generate input sizes
        // clip input sizes
        linearModel.samples[0] = sampleX.limit(numSamples).map( d -> Math.max(minFileSize, Math.abs(d)) ).toArray();

        // generate memory consumption
        // if slope = 0, we have independence

        // generate noise
        // y = error + ...
        linearModel.samples[1] = gaussian(0, errorStandardDeviation).limit(numSamples).toArray();
        for (int i = 0; i < numSamples; i++) {
            // ... slope * input + intercept
            linearModel.samples[1][i] += slope * linearModel.samples[0][i] + intercept;
            linearModel.samples[1][i] = Math.min(maxMemConsumption, Math.max(linearModel.samples[1][i], 10e6));
        }
        return linearModel;
    }

    public static LinearModel constant(double value, double errorStandardDeviation, double minValue){
        return new LinearModel(0, value, errorStandardDeviation, minValue);
    }

    private static double uniform(double lower, double upper){
        return Math.random()*(upper-lower)+lower;
    }

    private static DoubleStream gaussian(double mu, double sigma){
        return DoubleStream.generate(random::nextGaussian).map( normal -> sigma * normal + mu);
    }


    @Override
    public String toString() {
        return String.format("slope=%.2f, err sd=%.2f, mem min=%.2f, mem max=%.2f in MEGA}", slope, errorStandardDeviation/1e6, Arrays.stream(samples[1]).min().orElse(-1)/1e6, Arrays.stream(samples[1]).max().orElse(-1)/1e6);
    }


}