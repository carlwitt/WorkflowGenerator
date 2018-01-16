package simulation.generator;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.griphyn.cPlanner.code.generator.Abstract;
import org.griphyn.vdl.dax.Job;
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
    void generateWorkflows() throws Exception {

        Path targetDir = Paths.get("results", "random-memory-models");

        // avoid mixing up commas and dots when converting floating points to string (german vs. english locales)
        Locale.setDefault(new Locale("EN_us")); //Locale.setDefault();//setDefault(new Locale());

        // workflow classes
        List<Class<? extends AbstractApplication>> applicationClasses = new LinkedList<>();
        applicationClasses.add(Cybershake.class);
        applicationClasses.add(Genome.class);
        applicationClasses.add(Ligo.class);
        applicationClasses.add(Montage.class);
        applicationClasses.add(Sipht.class);

        // tasks per workflow instance
        int[] workflowSizes = {
                100,
                1000,
                10000,
        };

        // number of workflow instances per configuration (class, num tasks)
        // since each workflow has randomized runtimes, memory consumptions, etc. we want
        // more than one instance per configuration
        int numInstances = 3;

        // create a new workflow for each configuration (workflow type, num tasks, instance id)
        // write the dax output file
        // assemble workflow statistics
        for(Class<? extends AbstractApplication> appClass : applicationClasses){
            for (Integer numTasks : workflowSizes) {
                for (int instanceID = 0; instanceID < numInstances; instanceID++) {

                    // create a new Ligo/Cybershake/etc. object
                    AbstractApplication app = appClass.newInstance();
                    // create the workflow topology and sample the runtimes
                    app.generateWorkflow("-n", numTasks.toString());


                    // generate random memory models
                    for(String tasktype : app.getTasktypes()){
                        AppJob[] tasks = app.getTasks(tasktype);
                        LinearModel linearModel = LinearModel.randomMemoryModel(tasks.length, 10e3, 24e9, 0.5, 0.5,2.0);
//                        System.out.println(String.format("Task type: %s Memory model: %s", tasktype, linearModel));
                        app.memoryModels.put(tasktype, linearModel);
//                        System.out.println("tasks.length = " + tasks.length);
                        for (int i = 0; i < tasks.length; i++) {
                            tasks[i].addAnnotation("peak_mem_bytes", Long.toString((long) linearModel.getSamples()[1][i]));
                            long inputSize = (long) linearModel.getSamples()[0][i];
                            if(tasks[i].getInputs().size() == 0) {
                                System.err.printf("AppGeneratorTest.generateWorkflows: %s (%s, %s tasks, instance %s) has zero input files to distribute input size to", tasktype, appClass.getSimpleName(), numTasks, instanceID);
                                continue;
                            }
                            //assert tasks[i].getInputs().size() > 0 : String.format("Task %s has zero input files, failing to distribute input size %s", tasktype, inputSize);
                            long averageInputSize = inputSize / tasks[i].getInputs().size();
                            tasks[i].getInputs().forEach(appFilename -> appFilename.setSize(averageInputSize));
                        }
                    }

                    WorkflowStatistics statistics = app.getStatistics();

                    // write the workflow to text file (DAX format)
                    String filename = String.format("%s.n.%d.%d.dax", app.getClass().getSimpleName(), statistics.numberOfTasks, instanceID);
                    FileOutputStream fop = new FileOutputStream(new File(targetDir.resolve(filename).toString()));
                    app.printWorkflow(fop);
                    fop.close();

                    // add statistics for output in a file that describes the workflows
                    WorkflowStatistics.addStatistics(filename, statistics);

//                    for(String tasktype : app.getTasktypes()){
//                        System.out.printf("tasktype = %s (%s) instances%n", tasktype, statistics.numberOfTasksPerTaskType.get(tasktype));
//            System.out.println("numberOfTasksPerTaskType = " + statistics.numberOfTasksPerTaskType.get(tasktype));
//                        System.out.println("inputSizes = " + descriptiveStats(statistics.inputSizesPerTaskType.get(tasktype)));
//                        System.out.println("peakMem = " + descriptiveStats(statistics.memoryUsagesPerTaskType.get(tasktype)));
//                    }

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