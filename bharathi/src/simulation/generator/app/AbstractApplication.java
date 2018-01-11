package simulation.generator.app;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.griphyn.vdl.dax.ADAG;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import simulation.generator.util.Distribution;
import simulation.generator.util.LinearModel;

/**
 * @author Shishir Bharathi
 */
public abstract class AbstractApplication implements Application {

    private final ADAG dax;
    private int id;
    final Map<String, Distribution> distributions = new HashMap<>();
    public Map<String, LinearModel> memoryModels = new HashMap<>();

    AbstractApplication() {
        this.dax = new ADAG();
        this.id = 0;

    }
    
    protected Map<String, Distribution> getDistributions() {
        return this.distributions;
    }

    double generateDouble(String key) {
        Distribution dist = this.distributions.get(key);
        if (dist == null) {
            throw new RuntimeException("No such distribution: "+key);
        }
        return dist.getDouble();
    }

    long generateLong(String key) {
        return (long) generateDouble(key);
    }

    int generateInt(String key) {
        return (int) generateDouble(key);
    }

    protected abstract void populateDistributions();

    String getNewJobID() {
        return String.format("ID%05d", this.id++);
    }
    
    @Override
    public void printWorkflow(OutputStream os) throws Exception {
        this.dax.toXML(new OutputStreamWriter(os), "", null);
    }
    
    public ADAG getDAX() {
        return this.dax;
    }
    
    public void generateWorkflow(String... args) {
        populateDistributions();
        processArgs(args);
        constructWorkflow();
    }

    /** Returns an array with the names of all task types in the workflow.
     * E.g., "ExtractSGT", "SeismogramSynthesis", "PeakValCalcOkaya", "ZipSeis", "ZipPSA" for {@link Cybershake}.
     * This should match the names of the classes created for the tasks, e.g., {@link ExtractSGT}, {@link SeismogramSynthesis}, etc.
     * This is used to create a map that relates task types names to {@link LinearModel}s, as passed to {@link #generateWorkflow(Map, String...)}. */
    public abstract String[] getTasktypes();

    /** Generate a synthetic workflow with the same topology as the {@link AbstractApplication} but different resource usage characteristics.
     * This was used to generate the workflow suite used in Witt et al. 2018 */
    public void generateWorkflow(Map<String, LinearModel> memoryModels, String... args) {
        populateDistributions();
        // replace the default memory models initialized in populateDistributions()
        this.memoryModels = memoryModels;
        processArgs(args);
        constructWorkflow();
    }

    private LongStream getPeakMems(ADAG dax){
        Iterable iterable = dax::iterateJob;
        Stream<AppJob> targetStream = StreamSupport.stream(iterable.spliterator(), false);
        return targetStream.mapToLong(j -> Long.parseLong(j.getAnnotation("peak_mem_bytes")));
        // write out memory distributions
        // if(numTasks==2000){
        //     FileWriter fileWriter = new FileWriter("evaluation/sampled-peak-mem-"+app.getClass().getSimpleName()+".csv");
        //     Iterable iterable = app.getDAX()::iterateJob;
        //     Stream<AppJob> targetStream = StreamSupport.stream(iterable.spliterator(), false);
        //     fileWriter.write(String.format("task_type,input_size_total_bytes,peak_mem_bytes\n"));
        //     targetStream.forEach(j -> {
        //         try {
        //             fileWriter.write(String.format("%s,%s,%s%n",j.getName(),j.getAnnotation("input_total_bytes"),j.getAnnotation("peak_mem_bytes")));
        //         } catch (IOException e) {
        //             e.printStackTrace();
        //         }
        //     });
        //     fileWriter.close();
        // }
        // }
    }

    public WorkflowStatistics getStatistics(){

        WorkflowStatistics statistics = new WorkflowStatistics();

        Iterator<AppJob> iterable = getDAX().iterateJob();

        while (iterable.hasNext()) {

            AppJob next =  iterable.next();
            String tasktype = next.getClass().getSimpleName();

            // count number of tasks
            statistics.numberOfTasks++;
            statistics.numberOfTasksPerTaskType.merge(tasktype, 1, (old,diff)->old+1);

            // accumulate total task runtime
            double taskRuntimeSeconds = Double.parseDouble(next.getAnnotation("runtime"));
            assert taskRuntimeSeconds > 0 : String.format("Task runtime must be > 0, is %s for task %s", taskRuntimeSeconds, next.getClass().getSimpleName());

            statistics.totalRuntimeSeconds += taskRuntimeSeconds;

            // accumulate total task spacetime usage
            long taskMemoryBytes = Long.parseLong(next.getAnnotation("peak_mem_bytes"));
            assert taskMemoryBytes > 0 : String.format("Task peak memory consumption must be > 0, is %d for task %s", taskMemoryBytes, next.getClass().getSimpleName());
            double taskSpacetimeMegabyteSeconds = 1e-6 * taskMemoryBytes;
            statistics.totalSpacetimeMegabyteSeconds += taskRuntimeSeconds * taskSpacetimeMegabyteSeconds;

            // per task type statistics
            statistics.memoryUsagesPerTaskType.putIfAbsent(tasktype, new DescriptiveStatistics());
            statistics.memoryUsagesPerTaskType.get(tasktype).addValue(taskMemoryBytes);
            // accumulate total input file sizes (byte) per task type
            double sumOfInputs = next.getInputs().stream().mapToLong(AppFilename::getSize).sum();
            statistics.inputSizesPerTaskType.putIfAbsent(tasktype, new DescriptiveStatistics());
            statistics.inputSizesPerTaskType.get(tasktype).addValue(sumOfInputs);

        }

        // aggregate per task type statistics (min, max) into per workflow statistics
        for(String tasktype : getTasktypes()){

            // find minimum and maximum peak memory usage across task types
            statistics.maximumPeakMemoryBytes = (long) Math.max(statistics.maximumPeakMemoryBytes, statistics.memoryUsagesPerTaskType.get(tasktype).getMax());
            statistics.minimumPeakMemory= (long) Math.min(statistics.minimumPeakMemory, statistics.memoryUsagesPerTaskType.get(tasktype).getMin());

            System.out.println("tasktype = " + tasktype);
//            System.out.println("numberOfTasksPerTaskType = " + statistics.numberOfTasksPerTaskType.get(tasktype));
            System.out.println("inputSizes = " + descriptiveStats(statistics.inputSizesPerTaskType.get(tasktype)));
            System.out.println("peakMem = " + descriptiveStats(statistics.memoryUsagesPerTaskType.get(tasktype)));
        }

        return statistics;
    }

    private static String descriptiveStats(DescriptiveStatistics s){
        return String.format("[%s, %s] µ=%s, σ=%s in MEGA", s.getMin()/1e6, s.getMax()/1e6, s.getMean()/1e6, s.getStandardDeviation()/1e6);
    }
    
    protected abstract void processArgs(String[] args);
    protected abstract void constructWorkflow();


}
