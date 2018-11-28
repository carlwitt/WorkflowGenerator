/* ******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Carl Witt (HU Berlin)
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
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

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.griphyn.vdl.dax.PseudoText;
import simulation.generator.app.*;
import simulation.generator.util.LinearModel;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
/**
 * Created by Carl Witt on 11/28/18.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class GenerateCorpus {

    public static void main(String[] args) throws Exception {

        // TODO the distributions of the workflow generator (by Bharathi) do not exactly match the published numbers in Juve 2013 (FGCS)
        // for instance the TmpltBank task type has a mean runtime of 20 seconds in the generator vs 500 sec in the publication
        // also, some of the workflow's task types as published do not appear in the simulation

        Path targetDir = Paths.get("results", "rmm10");

        // avoid mixing up commas and dots when converting floating points to string (german vs. english locales)
        Locale.setDefault(new Locale("EN_us")); //Locale.setDefault();//setDefault(new Locale());

        Random random = new Random(1L);
        // workflow classes
        List<Class<? extends AbstractApplication>> applicationClasses = new LinkedList<>();
        applicationClasses.add(Cybershake.class);
        applicationClasses.add(Genome.class);
        applicationClasses.add(Ligo.class);
        applicationClasses.add(Montage.class);
        applicationClasses.add(Sipht.class);

        // tasks per workflow instance
        int[] workflowSizes = {
//                100,
//                200,
                1000,
//                5000,
//                10000,
        };

        // number of workflow instances per configuration (class, num tasks)
        // since each workflow has randomized runtimes, memory consumptions, etc. we want
        // more than one instance per configuration
        int numInstances = 200;

        // create a new workflow for each configuration (workflow type, num tasks, instance id)
        // write the dax output file
        // assemble workflow statistics
        for(Class<? extends AbstractApplication> appClass : applicationClasses){
            for (Integer workflowSize : workflowSizes) {
                for (int instanceID = 0; instanceID < numInstances; instanceID++) {

                    WorkflowStatistics statistics;
                    AbstractApplication app;

                    // create a new Ligo/Cybershake/etc. object
                    app = appClass.newInstance();
                    // create the workflow topology and sample the runtimes
                    app.generateWorkflow("-n", workflowSize.toString());

                    // generate random memory model for each task type
                    for (String tasktype : app.getTasktypes()) {

                        // get tasks of current type
                        AppJob[] tasks = app.getTasks(tasktype);

                        // random memory model
                        double minFileSize = 10e3;
                        double maxMemConsumption = 1024e9;
                        double linearTaskChance = 0.5;
                        double minSlope = 1;
                        double maxSlope = 10.0;
                        LinearModel linearModel = LinearModel.randomMemoryModel(tasks.length, minFileSize, maxMemConsumption, linearTaskChance, minSlope, maxSlope);

                        app.memoryModels.put(tasktype, linearModel);

                        // annotate tasks of current type
                        for (int i = 0; i < tasks.length; i++) {

                            // add memory consumption both as XML element attribute and (as a compatibility hack, as a separate <argument> element)
                            long peakMemoryConsumptionByte = (long) linearModel.getSamples()[1][i];
                            // normally distributed and capped to range [0, 1]
                            double peakMemoryRelativeTime = Math.min(1, Math.max(0, random.nextDouble()*0.7+0.3));

                            tasks[i].addAnnotation("peak_mem_bytes", Long.toString(peakMemoryConsumptionByte));
                            tasks[i].addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemoryConsumptionByte, peakMemoryRelativeTime)));

                            double[] filesizes = linearModel.getSamples()[0];
                            if (tasks[i].getInputs().size() == 0) {
                                System.err.printf("AppGeneratorTest.generateWorkflows: %s (%s.n.%s.%s.dax) has zero input files to distribute input size to%n", tasktype, appClass.getSimpleName(), workflowSize, instanceID);
                                continue;
                            }
                            long averageInputSize = ((long) filesizes[i]) / tasks[i].getInputs().size();
                            tasks[i].getInputs().forEach(appFilename -> appFilename.setSize(averageInputSize));
                        }
                    }
                    statistics = app.getStatistics();

                    // write the workflow to text file (DAX format)
                    String filename = String.format("%s.n.%d.%d.dax", app.getClass().getSimpleName(), statistics.numberOfTasks, instanceID);
                    FileOutputStream fop = new FileOutputStream(new File(targetDir.resolve(filename).toString()));
                    app.printWorkflow(fop);
                    fop.close();

                    // add statistics for output in a file that describes the workflows
                    WorkflowStatistics.addStatistics(filename, statistics);

                    for(String tasktype : app.getTasktypes()){
                        DescriptiveStatistics memory = statistics.memoryUsagesPerTaskType.get(tasktype);
                        System.out.printf("%s,%s,%s,%s,%s,%.2f,%.2f %n", app.getClass().getSimpleName(), workflowSize, instanceID, tasktype, statistics.numberOfTasksPerTaskType.get(tasktype), memory.getMean()/1e9, memory.getStandardDeviation()/1e9);
//            System.out.println("numberOfTasksPerTaskType = " + statistics.numberOfTasksPerTaskType.get(tasktype));
//                        System.out.println("inputSizes = " + descriptiveStats(statistics.inputSizesPerTaskType.get(tasktype)));
//                        System.out.println("peakMem = " + descriptiveStats(statistics.memoryUsagesPerTaskType.get(tasktype)));
                    }

                }
            }
        }

        // write summary file that describes all generated workflows (e.g., their number of tasks, memory models, etc.)
        WorkflowStatistics.writeStatisticsCSV(targetDir.resolve("workflowStatistics.csv").toString());

    }

}
