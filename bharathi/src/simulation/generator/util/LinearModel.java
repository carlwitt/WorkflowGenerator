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
public class LinearModel {

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

    /** Pairs of (total file input size, peak memory consumption) generated in {@link #randomMemoryModel(int, double, double, double, double, double)}}.
     * The samples[0] is the array of input sizes, samples[1] is the array of peak memory usages. */
    private double[][] samples = new double[2][];

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

    public static LinearModel constant(double value, double errorStandardDeviation, double minValue){
        return new LinearModel(0, value, errorStandardDeviation, minValue);
    }

    /**
     * @return an array of random input sizes and possibly correlated memory consumption, depending on the parameters generated in {@link #randomMemoryModel(int, double, double, double, double, double)}
     */
    public double[][] getSamples(){
        return samples;
    }

    @Override
    public String toString() {
        return String.format("slope=%.2f, err sd=%.2f, mem min=%.2f, mem max=%.2f in MEGA}", slope, errorStandardDeviation/1e6, Arrays.stream(samples[1]).min().orElse(-1)/1e6, Arrays.stream(samples[1]).max().orElse(-1)/1e6);
    }

    private static double uniform(double lower, double upper){
        return Math.random()*(upper-lower)+lower;
    }

    private static Random random = new Random();

    private static DoubleStream gaussian(double mu, double sigma){
        return DoubleStream.generate(random::nextGaussian).map( normal -> sigma * normal + mu);
    }

    /** This was used to generate the random memory models for each task type in Witt et al. 2018.
     * Initializes the {@link #samples} array. */
    public static LinearModel randomMemoryModel(int numSamples, double minFileSize, double maxMemConsumption, double linearTaskChance, double minSlope, double maxSlope){

        // input range orientation between 100MB and 1GB (actual input sizes vary beyond these limits), but
        // this brings the input sizes into a good range
        double minInput = 100e6;
        double maxInput = 1e9;

        // average memory usage between 10 MB and maximum memory consumption
        double meanY = uniform(10e6, maxMemConsumption);
        // standard deviation between 3% and 10% of the mean (seems low, but produces realistic feeling models; otherwise we get very large memory ranges)
        double varY = Math.pow(meanY * uniform(0.03, 0.1), 2.0);
        // zero slope in half of the cases, between 0.5 and 2 in the rest
        double slope = Math.random() > linearTaskChance ? 0. : uniform(minSlope, maxSlope);
        double intercept;
        double errorStandardDeviation;

        // the input sizes
        DoubleStream sampleX;

        if (slope < 1e-6) {
            // since the input size has no effect on the output, it can be anything
            intercept = meanY;
            // all of the variance is caused by errors (i.e., is unexplained, since input size is the only explanatory factor we consider)
            errorStandardDeviation = Math.sqrt(varY);
            sampleX = gaussian((minInput+maxInput)/2.0, (maxInput-minInput)/3.0);
        } else {
            intercept = 0; // for simplicity, could also go for something like mu - (minInput + maxInput) / 2.;

            // the linearity determines the amount of output variable variance explained by the input (is related but not the same as correlation)
            double linearity = uniform(0.25, 0.75);

            // the amount of variance explained by input
            double varYByInput = linearity * varY;
            // the unexplained amount of variance
            double varYUnexplained = (1.-linearity) * varY;

            // the variance of the input distribution depends on the variance of the output (more specifically, the variance explained by input)
            double varX = varYByInput/Math.pow(slope, 2.0); // since Var[mX] = m^2 Var[X]

            double meanX = meanY / slope - intercept; // since E[mX+n] = m*E[X] + n

            errorStandardDeviation = Math.sqrt(varYUnexplained);

            sampleX = gaussian(meanX, Math.sqrt(varX));

        }
        LinearModel linearModel = new LinearModel(slope, intercept, errorStandardDeviation, 30e6);

        // generate samples
        linearModel.samples[0] = sampleX.map( d -> Math.max(minFileSize, Math.abs(d)) ).limit(numSamples).toArray();

        // y = error + ...
        linearModel.samples[1] = gaussian(0, errorStandardDeviation).limit(numSamples).toArray();
        for (int i = 0; i < numSamples; i++) {
            // ... slope * input + intercept
            linearModel.samples[1][i] = Math.min(maxMemConsumption, slope * linearModel.samples[0][i] + intercept + linearModel.samples[1][i]);
        }

        return linearModel;
    }

    /**
     * peak mem = slope * input size + intercept + random value sampled from Normal(0, errorStandardDeviation^2)
     * @param inputFileSize The size of the input file, usually in bytes.
     * @return a random value ≥ {@link #minValue} according to the model.
     */
    public long generate(long inputFileSize){
        return (long) Math.max(minValue, inputFileSize*slope + intercept + error.nextGaussian()*errorStandardDeviation);
    }


}