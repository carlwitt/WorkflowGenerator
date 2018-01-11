/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
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
package simulation.generator.app;

import jdk.nashorn.internal.runtime.WithObject;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.griphyn.vdl.workflow.Workflow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Contains descriptive statistics of a single workflow instance.
 * This includes topological aspects (number of tasks) and resource consumption (e.g., sequential execution time).
 *
 * Created by Carl Witt on 1/11/18.
 *
 * @author Carl Witt (cpw@posteo.de)
 */
public class WorkflowStatistics {

    // aggregate statistics
    /** The number of jobs (=tasks=vertices in the DAG) in the workflow. */
    public int numberOfTasks;
    /** The sum of the runtime of each task in the workflow in seconds (sequential execution duration). */
    public double totalRuntimeSeconds;
    /** The accumulated megabyteseconds (runtime * peak memory consumption in megabytes) of all tasks. */
    public double totalSpacetimeMegabyteSeconds;
    /** The smallest peak memory consumed by any task, in bytes. */
    public long minimumPeakMemory = Long.MAX_VALUE;
    /** The largest peak memory consumed by any task, in bytes. */
    public long maximumPeakMemoryBytes = 0L;

    // per tasktype statistics
    /** For each task type, gives the mean average over the input file size sums. */
    public Map<String, Integer> numberOfTasksPerTaskType = new HashMap<>();
    /** For each task type, gives the mean average over the input file size sums. */
    public Map<String, DescriptiveStatistics> inputSizesPerTaskType = new HashMap<>();
    /** Summary statistics over the distribution of peak memory usages within a task type (min, max, mean, sd, etc.)*/
    public Map<String, DescriptiveStatistics> memoryUsagesPerTaskType = new HashMap<>();

    // workflow corpus statistics
    /** See {@link #addStatistics(String, WorkflowStatistics)} */
    private static Map<String, WorkflowStatistics> statistics = new LinkedHashMap<>();

    /** Used to assemble and output statistics for a collection of workflows.
     * @param filename name of the file containing the workflow (task resource usage and dependencies in DAX format)
     * @param statistics the descriptive statistics of the workflow in that file
     */
    public static void addStatistics(String filename, WorkflowStatistics statistics){
        WorkflowStatistics.statistics.put(filename, statistics);
    }

    /** Used to write the assembled workflow statistics to a csv file. */
    public static void writeStatisticsCSV(String filename) throws IOException {
        // contains the csv line per workflow file
        StringBuffer workflowStatisticsCsv = new StringBuffer();
        workflowStatisticsCsv.append("file,num_tasks,total_runtime_seconds,total_spacetime_megabyteseconds,minimum_peak_memory_mb,maximum_peak_memory_mb\n");

        for(String workflowFileName : statistics.keySet()){
            WorkflowStatistics s = statistics.get(workflowFileName);
            workflowStatisticsCsv.append(String.format("%s,%s,%s,%s,%s,%s\n", workflowFileName, s.numberOfTasks, s.totalRuntimeSeconds, s.totalSpacetimeMegabyteSeconds, 1e-6*s.minimumPeakMemory, 1e-6*s.maximumPeakMemoryBytes));
        }

        Files.write(Paths.get(filename), workflowStatisticsCsv.toString().getBytes());

    }

}
