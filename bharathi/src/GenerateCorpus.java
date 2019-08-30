/*
 *
 */

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

        // scale workflows to this area (Terabyte-Weeks as measured in accumulated runtime times accumulated peak memory usage)
        double targetTibWeeks = 1.0;

        // TODO the distributions of the workflow generator (by Bharathi) do not exactly match the published numbers in Juve 2013 (FGCS)
        // for instance the TmpltBank task type has a mean runtime of 20 seconds in the generator vs 500 sec in the publication
        // also, some of the workflow's task types as published do not appear in the simulation

        Path targetDir = Paths.get(args[0]);
        // doesn't work.
        if(! targetDir.toFile().exists() && ! targetDir.toFile().mkdir()){
            System.out.println("Couldn't create dir "+targetDir);
            System.out.println("targetDir.toFile() = " + targetDir.toFile());
            System.exit(-1);
        }
        System.out.println("Writing workflows to "+targetDir.toAbsolutePath().toString());

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
        int numInstances = 100;

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
                        double maxMemConsumption = 1.5e12;
                        double linearTaskChance = 0.5;
                        double minSlope = 0.2;
                        double maxSlope = 2;
                        LinearModel linearModel = LinearModel.randomMemoryModel(tasks.length, minFileSize, maxMemConsumption, linearTaskChance, minSlope, maxSlope);

                        app.memoryModels.put(tasktype, linearModel);

                        // annotate tasks of current type
                        for (int i = 0; i < tasks.length; i++) {

                            // add memory consumption both as XML element attribute and (as a dax specification compatibility hack, as a separate <argument> element)
                            long peakMemoryConsumptionByte = (long) linearModel.getSamples()[1][i];

                            // fixed middle ground between optimistic (0) and pessimistic (1)
                            double peakMemoryRelativeTime = 0.5;
                            // normally distributed and capped to range [0, 1]
//                            double peakMemoryRelativeTime = Math.min(1, Math.max(0, random.nextDouble()*0.7+0.3));

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

                    // scale the workflows to a uniform amount of resources
                    double tibWeeks = statistics.totalSpacetimeMegabyteSeconds / 1024. / 1024. / 3600. / 24. / 7.;
                    System.out.println("TBw before normalization = " + tibWeeks);

                    double scaleFactor = tibWeeks / targetTibWeeks;
                    for(String type: app.getTasktypes()){
                        for(AppJob task: app.getTasks(type)){
                            double scaledRuntime = Double.parseDouble(task.getAnnotation("runtime")) / scaleFactor;
                            task.addAnnotation("runtime", String.valueOf(scaledRuntime));
                        }
                    }

                    // update app statistics
                    statistics = app.getStatistics();
                    double tibWeeks2 = statistics.totalSpacetimeMegabyteSeconds / 1024. / 1024. / 3600. / 24. / 7.;
                    System.out.println("TBw after normalization = " + tibWeeks2);

                    // write the workflow to text file (DAX format)
                    String filename = String.format("%s.n.%d.%d.dax", app.getClass().getSimpleName(), statistics.numberOfTasks, instanceID);
                    FileOutputStream fop = new FileOutputStream(new File(targetDir.resolve(filename).toString()));
                    app.printWorkflow(fop);
                    fop.close();

                    // add this workflow's statistics to the corpus currently being generated for later writing a file that describes all workflows
                    WorkflowStatistics.addStatistics(filename, statistics);

                    for(String tasktype : app.getTasktypes()) {
                        DescriptiveStatistics memory = statistics.memoryUsagesPerTaskType.get(tasktype);
                        System.out.printf("%s,%s,%s,%s,%s,µ=%.2f,s=%.2f,peak = %.2f%n", app.getClass().getSimpleName(), workflowSize, instanceID, tasktype, statistics.numberOfTasksPerTaskType.get(tasktype), memory.getMean() / 1e9, memory.getStandardDeviation() / 1e9, memory.getMax() / 1e9);
//            System.out.println("numberOfTasksPerTaskType = " + statistics.numberOfTasksPerTaskType.get(tasktype));
//                        System.out.println("inputSizes = " + descriptiveStats(statistics.inputSizesPerTaskType.get(tasktype)));
//                        System.out.println("peakMem = " + descriptiveStats(statistics.memoryUsagesPerTaskType.get(tasktype)));
                    } //task type info

                } //instance



            } // workflow size

        } // workflow type

        // write summary file that describes all generated workflows (e.g., their number of tasks, memory models, etc.)
        WorkflowStatistics.writeStatisticsCSV(targetDir.resolve("workflowStatistics.csv").toString());

    }

}
