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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Pattern;
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
public class BolandModel extends MemoryModel {

    static TaskInstance[] data;
    static {
        data = parseData();
    }

    protected static class TaskInstance{
        String lineage1, lineage2, lineage3, lineage4, lineage5;
        double base_count, read_count, compressed_data_size;
        String library_layout, library_strategy, library_source, name;
        double execution_time, peak_mem_gb;

        @Override
        public String toString() {
            return "TaskInstance{" +
                    "lineage1='" + lineage1 + '\'' +
                    ", lineage2='" + lineage2 + '\'' +
                    ", lineage3='" + lineage3 + '\'' +
                    ", lineage4='" + lineage4 + '\'' +
                    ", lineage5='" + lineage5 + '\'' +
                    ", base_count=" + base_count +
                    ", read_count=" + read_count +
                    ", compressed_data_size=" + compressed_data_size +
                    ", library_layout='" + library_layout + '\'' +
                    ", library_strategy='" + library_strategy + '\'' +
                    ", library_source='" + library_source + '\'' +
                    ", name='" + name + '\'' +
                    ", execution_time=" + execution_time +
                    ", peak_mem_gb=" + peak_mem_gb +
                    '}';
        }

        public TaskInstance(String lineage1, String lineage2, String lineage3, String lineage4, String lineage5,
                            String base_count, String read_count, String compressed_data_size,
                            String library_layout, String library_strategy, String library_source, String name, String execution_time, String peak_mem_gb) {
            try{

                this.lineage1 = lineage1;
                this.lineage2 = lineage2;
                this.lineage3 = lineage3;
                this.lineage4 = lineage4;
                this.lineage5 = lineage5;
                this.base_count = Double.parseDouble(base_count);
                this.read_count = Double.parseDouble(read_count);
                this.compressed_data_size = Double.parseDouble(compressed_data_size);
                this.library_layout = library_layout;
                this.library_strategy = library_strategy;
                this.library_source = library_source;
                this.name = name;
                this.execution_time = Double.parseDouble(execution_time);
                this.peak_mem_gb = Double.parseDouble(peak_mem_gb);
            } catch (Exception e){
                System.out.println("e = " + e);
                System.exit(-1);
            }
        }
    }

    public BolandModel(int numTasks, int userId, long randomSeed){
//        Random random = new Random(randomSeed);
//        for (int i = 0; i < numTasks; i++) {
//            int randomOffset = random.nextInt(data.length);
//            TaskInstance randomInstance = data[randomOffset];
//            Task t = new Task("assembly", ""+randomOffset, this, userId, i,
//                    // execution times are cumulative measurements for 32 cores
//                    // multiply with 1,000,000 to get seconds back when running on standard CPUs
//                    (long) (randomInstance.execution_time/32.*1e6), 0, 0, 1, 0, 0, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull()
//            );
//            t.setPeakMemoryBytes((long) (randomInstance.peak_mem_gb*1e9));
//            addTask(t);
//
//
//        }
    }

    static protected TaskInstance[] parseData(){
        Scanner scanner = null;
        TaskInstance result[] = new TaskInstance[24580];

        try {
            scanner = new Scanner(new File("profiling-data/boland-assembly-data.csv"));

            // skip header
            scanner.nextLine();

            scanner.useDelimiter(Pattern.compile("[,\n]"));
            int i = 0;
            while(scanner.hasNext()){
//                System.out.print(scanner.next()+"|");
                result[i++] = new TaskInstance(scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next(),scanner.next());
            }
//            System.out.println("result[0] = " + result[0]);
//            System.out.println("result[1] = " + result[1]);
//            System.out.println("result[24579] = " + result[24579]);
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public long generate(long inputFileSize) {
        System.out.println("BolandModel.generate");
        System.out.println("inputFileSize = [" + inputFileSize + "]");
        throw new NotImplementedException();
    }

}