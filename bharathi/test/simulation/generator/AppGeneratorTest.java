package simulation.generator;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.griphyn.cPlanner.code.generator.Abstract;
import org.griphyn.vdl.dax.Job;
import org.griphyn.vdl.dax.PseudoText;
import org.junit.jupiter.api.Test;
import simulation.generator.app.*;
import simulation.generator.util.LinearModel;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by Carl Witt on 03.11.17.
 *
 * The dax.new2 format added relative time to failure values (I think).
 * The dax.new3 format switched from a uniform error model to a normally distributed error model.
 * The workflows in random-memory-models/ employ
 *
 */
class AppGeneratorTest {

    @Test
    void main() throws Exception {

        // avoid setting getting comma decimal separators for German locale
        Locale.setDefault(new Locale("EN_us")); //Locale.setDefault();//setDefault(new Locale());

        Application app = new Cybershake();

        String[] args = new String[]{
//            "--data", "100",                   // -d Approximate size of input data.
//            "--factor", "100",                 // -f Avg. runtime to execute an mProject job.
//            "--inputs", "100",                 // -i Number of inputs.
                "-n", "20",                // -n Number of jobs.
//            "--overlap-probability", "0.5",    // -p Probability any two inputs overlap.
//            "--square", "100"                  // -s Square degree of workflow.
        };

        app.generateWorkflow(args);

        Iterator iterator = app.getDAX().iterateJob();
        while (iterator.hasNext()) {
            Job next = (Job) iterator.next();
            System.out.printf("job %s\n%s%n", next.getName(), next.getArgument(0));
        }
//        app.printWorkflow(System.out);
    }

    @Test
    void generateYarnStarvationTest() throws Exception {

        for(String minute : new String[]{"3", "25", "500"}){

            // generate workflow
            YarnStarvationTest test = new YarnStarvationTest();
            test.generateWorkflow(minute);

            // write to file
            String filename = String.format("results/yarn-starvation-test/yarn-starvation-v3-%s-minutes.dax", minute);
            FileOutputStream fop = new FileOutputStream(new File(filename));
            test.printWorkflow(fop);
            fop.close();
        }

    }

    @Test
    void generateSipht() throws Exception{
        Sipht sipht = new Sipht();
        sipht.generateWorkflow("-n", "400");
        sipht.printWorkflow(System.out);

    }

    @Test
    //TODO doesn't work.
    void generateVariantCalling() throws Exception{
        Path targetDir = Paths.get("results", "variant-calling");

        // avoid mixing up commas and dots when converting floating points to string (german vs. english locales)
        Locale.setDefault(new Locale("EN_us")); //Locale.setDefault();//setDefault(new Locale());

        Random random = new Random(1L);

        // tasks per workflow instance
        int[] workflowSizes = {
                100,
                2500,
                5000,
//                10000,
        };

        // number of workflow instances per configuration (class, num tasks)
        // since each workflow has randomized runtimes, memory consumptions, etc. we want
        // more than one instance per configuration
        int numInstances = 1;

        // create a new workflow for each configuration (workflow type, num tasks, instance id)
        // write the dax output file
        // assemble workflow statistics
        for (Integer workflowSize : workflowSizes) {
            for (int instanceID = 0; instanceID < numInstances; instanceID++) {

                WorkflowStatistics statistics;
                AbstractApplication app;

                do{

                    // create a new Ligo/Cybershake/etc. object
                    app = new VariantCalling();
                    // create the workflow topology and sample the runtimes
                    app.generateWorkflow("-n", workflowSize.toString());

                    // generate random memory model for each task type
                    for (String tasktype : app.getTasktypes()) {

                        // get tasks of current type
                        AppJob[] tasks = app.getTasks(tasktype);

                        // random memory model
                        double minFileSize = 10e3;
                        double maxMemConsumption = 64e9;
                        double linearTaskChance = 0.5;
                        double minSlope = 1;
                        double maxSlope = 10.0;
                        LinearModel linearModel = LinearModel.randomMemoryModel(tasks.length, minFileSize, maxMemConsumption, linearTaskChance, minSlope, maxSlope);

                        // random failure model
                        double peakMemoryRelativeTimeMean = random.nextDouble();
                        double peakMemoryRelativeTimeSd = random.nextDouble()/2.;

                        app.memoryModels.put(tasktype, linearModel);

                        // annotate tasks of current type
                        for (int i = 0; i < tasks.length; i++) {

                            // add memory consumption both as XML element attribute and (as a compatibility hack, as a separate <argument> element)
                            long peakMemoryConsumptionByte = (long) linearModel.getSamples()[1][i];
                            // normally distributed and capped to range [0, 1]
                            double peakMemoryRelativeTime = Math.min(1, Math.max(0, random.nextGaussian()*peakMemoryRelativeTimeSd+peakMemoryRelativeTimeMean));

                            tasks[i].addAnnotation("peak_mem_bytes", Long.toString(peakMemoryConsumptionByte));
                            tasks[i].addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemoryConsumptionByte, peakMemoryRelativeTime)));

                            double[] filesizes = linearModel.getSamples()[0];
                            if (tasks[i].getInputs().size() == 0) {
                                System.err.printf("AppGeneratorTest.generateWorkflows: %s (%s.n.%s.%s.dax) has zero input files to distribute input size to%n", tasktype, VariantCalling.class.getSimpleName(), workflowSize, instanceID);
                                continue;
                            }
                            long averageInputSize = ((long) filesizes[i]) / tasks[i].getInputs().size();
                            tasks[i].getInputs().forEach(appFilename -> appFilename.setSize(averageInputSize));
                        }
                    }

                    statistics = app.getStatistics();
                } while (statistics.memoryHeterogeneity >= 0.3 && statistics.cpuToMemRatio < 0.5);

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

        // write summary file that describes all generated workflows (e.g., their number of tasks, memory models, etc.)
        WorkflowStatistics.writeStatisticsCSV(targetDir.resolve("workflowStatistics.csv").toString());
    }

    @Test
    void generateWorkflows() throws Exception {

        // TODO the distributions of the workflow generator (by Bharathi) do not exactly match the published numbers in Juve 2013 (FGCS)
        // for instance the TmpltBank task type has a mean runtime of 20 seconds in the generator vs 500 sec in the publication
        // also, some of the workflow's task types as published do not appear in the simulation

        Path targetDir = Paths.get("results", "rmm9");

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
                200,
//                5000,
//                10000,
        };

        // number of workflow instances per configuration (class, num tasks)
        // since each workflow has randomized runtimes, memory consumptions, etc. we want
        // more than one instance per configuration
        int numInstances = 3;

        // create a new workflow for each configuration (workflow type, num tasks, instance id)
        // write the dax output file
        // assemble workflow statistics
        for(Class<? extends AbstractApplication> appClass : applicationClasses){
            for (Integer workflowSize : workflowSizes) {
                for (int instanceID = 0; instanceID < numInstances; instanceID++) {

                    WorkflowStatistics statistics;
                    AbstractApplication app;

                    do{

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
                            double maxMemConsumption = 64e9;
                            double linearTaskChance = 0.5;
                            double minSlope = 1;
                            double maxSlope = 10.0;
                            LinearModel linearModel = LinearModel.randomMemoryModel(tasks.length, minFileSize, maxMemConsumption, linearTaskChance, minSlope, maxSlope);

                            // random failure model
                            double peakMemoryRelativeTimeMean = random.nextDouble();
                            double peakMemoryRelativeTimeSd = random.nextDouble()/2.;

                            app.memoryModels.put(tasktype, linearModel);

                            // annotate tasks of current type
                            for (int i = 0; i < tasks.length; i++) {

                                // add memory consumption both as XML element attribute and (as a compatibility hack, as a separate <argument> element)
                                long peakMemoryConsumptionByte = (long) linearModel.getSamples()[1][i];
                                // normally distributed and capped to range [0, 1]
                                double peakMemoryRelativeTime = Math.min(1, Math.max(0, random.nextGaussian()*peakMemoryRelativeTimeSd+peakMemoryRelativeTimeMean));

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
                    } while (statistics.memoryHeterogeneity >= 0.3 && statistics.cpuToMemRatio < 0.5);

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

    private static String descriptiveStats(DescriptiveStatistics s){
        return String.format("[%s, %s] µ=%s, σ=%s in MEGA", s.getMin()/1e6, s.getMax()/1e6, s.getMean()/1e6, s.getStandardDeviation()/1e6);
    }

}