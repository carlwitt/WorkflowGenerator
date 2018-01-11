package simulation.generator;

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
//        applicationClasses.add(Genome.class);
//        applicationClasses.add(Ligo.class);
//        applicationClasses.add(Montage.class);
//        applicationClasses.add(Sipht.class);

        // tasks per workflow instance
        int[] workflowSizes = {
//                500,
                1000,
//                2000,
        };

        // number of workflow instances per configuration (class, num tasks)
        // since each workflow has randomized runtimes, memory consumptions, etc. we want
        // more than one instance per configuration
        int numInstances = 2;

        // create a new application for each configuration (workflow type, num tasks, instance id)
        List<AbstractApplication> applications = new LinkedList<>();
        for(Class<? extends AbstractApplication> appClass : applicationClasses){
            for (Integer numTasks : workflowSizes) {
                for (int j = 0; j < numInstances; j++) {
                    // create a new Ligo/Cybershake/etc. object
                    AbstractApplication e = appClass.newInstance();

                    // sample memory model parameters
                    for(String tasktype : e.getTasktypes()){
                        double slope = Math.random() * 10;
                        double intercept = Math.random() * 24e9;
                        double errorStandardDeviation = Math.random() * 10e9;
                        System.out.printf("mem(%s) = %sx + %s + N(0, %s) in MEGA%n", tasktype, slope/1e6, intercept/1e6, errorStandardDeviation/1e6);
                        e.memoryModels.put(tasktype, new LinearModel(slope, intercept, errorStandardDeviation, 10e6));
                    }

                    // create the workflow topology and sample their resource requirements
                    e.generateWorkflow("-n", numTasks.toString());
                    applications.add(e);
                }
            }
        }


        int instance = 0;
        for (AbstractApplication app : applications) {

            WorkflowStatistics statistics = app.getStatistics();

            // write the workflow to text file (DAX format)
            String filename = String.format("%s.n.%d.%d.dax", app.getClass().getSimpleName(), statistics.numberOfTasks, instance);
            FileOutputStream fop = new FileOutputStream(new File(targetDir.resolve(filename).toString()));
            app.printWorkflow(fop);
            fop.close();

            // add statistics for output in a file that describes the workflows
            WorkflowStatistics.addStatistics(filename, statistics);
            instance = (instance+1)%numInstances;

        }

        // write summary file that describes all generated workflows (e.g., their number of tasks, memory models, etc.)
        WorkflowStatistics.writeStatisticsCSV(targetDir.resolve("workflowStatistics.csv").toString());
    }


}