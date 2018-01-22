package simulation.generator.app;

import org.griphyn.vdl.dax.PseudoText;
import simulation.generator.util.Distribution;
import simulation.generator.util.LinearModel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class VariantCalling extends AbstractApplication {

    private int gunzips = 2;
    private int paths   = 25;

    static final String NAMESPACE = "VariantCalling";

    private void usage(int exitCode) {

        String msg = "VariantCalling [-h] [options]." +
                "\n--help | -h Print help message." +
                "\n--numpaths | -n Number of paths.";

        System.out.println(msg);
        System.exit(exitCode);
    }

    double getRuntimeFactor() {
        return 1.;
    }

    @Override
    protected void processArgs(String[] args) {
        //TODO
    }

    public void constructWorkflow() {

        // The root tasks
        VC_Untar untar = new VC_Untar(this, "untar", "1.0", getNewJobID());

        Set<VC_Gunzip> gunzipSet = new HashSet<>();

        for (int i = 0; i < this.gunzips; i++)
            gunzipSet.add(new VC_Gunzip(this, "gunzip", "1.0", getNewJobID()));

        // Consuming tasks
        VC_Annovar annovar = new VC_Annovar(this, "annovar", "1.0", getNewJobID());

        for (VC_Gunzip g : gunzipSet) {

            VC_Fastqc fastqc = new VC_Fastqc(this, "fastqc", "1.0", getNewJobID());
            g.addChild(fastqc);
            fastqc.finish();

        }

        for (int i = 0; i < this.paths; i++) {

            // Tasks along this path
            VC_Faidx   faidx   = new VC_Faidx(this, "faidx", "1.0", getNewJobID());
            VC_Build   build   = new VC_Build(this, "build", "1.0", getNewJobID());
            VC_Align   align   = new VC_Align(this, "align", "1.0", getNewJobID());
            VC_Sort    sort    = new VC_Sort(this, "align", "1.0", getNewJobID());
            VC_Pileup  pileup  = new VC_Pileup(this, "align", "1.0", getNewJobID());
            VC_Varscan varscan = new VC_Varscan(this, "align", "1.0", getNewJobID());

            // VC_Build the path
            for (VC_Gunzip g : gunzipSet)
                g.addChild(align);

            untar.addChild(faidx, i);
            untar.addChild(build, i);
            untar.addChild(pileup, i);
            build.addChild(align, i);
            align.addChild(sort, i);
            sort.addChild(pileup, i);
            faidx.addChild(pileup, i);
            pileup.addChild(varscan, i);
            varscan.addChild(annovar, i);

            // Compute all the attribute values
            faidx.finish();
            build.finish();
            align.finish();
            sort.finish();
            pileup.finish();
            varscan.finish();

        }

        annovar.finish();

    }

    @Override
    protected void populateDistributions() {

        //TODO: Correct distributions

        /*
         * File size distributions.
         */
        distributions.put("UNTAR_INPUT", Distribution.getConstantDistribution(3273.502720e6));
        distributions.put("UNTAR_OUTPUT", Distribution.getTruncatedNormalDistribution(130939246.0, 3.20135891361e+15));
        distributions.put("GUNZIP_INPUT", Distribution.getConstantDistribution(55));
        distributions.put("GUNZIP_OUTPUT", Distribution.getConstantDistribution(645395878.0));

        // Replace constant distributions with linear constant with variation
        // faidx -> ~23 but one is 17903 -> one path seems to be different
        distributions.put("FAIDX_OUTPUT", Distribution.getTruncatedNormalDistribution(738.4, 12275979.12));
        distributions.put("BUILD_OUTPUT", Distribution.getTruncatedNormalDistribution(181196390.4, 6.70218223337e+15));
        distributions.put("ALIGN_OUTPUT", Distribution.getTruncatedNormalDistribution(26618855.28, 1.60339463157e+14));
        distributions.put("SORT_OUTPUT", Distribution.getTruncatedNormalDistribution(22393430.44, 1.11119046719e+14));
        distributions.put("PILEUP_OUTPUT", Distribution.getTruncatedNormalDistribution(116875582.56, 3.52492963783e+15));
        distributions.put("VARSCAN_OUTPUT", Distribution.getTruncatedNormalDistribution(606048.04, 82585810907.9));

        distributions.put("FASTQC_OUTPUT", Distribution.getTruncatedNormalDistribution(444675.5, 389292630.25));
        distributions.put("ANNOVAR_OUTPUT", Distribution.getConstantDistribution(6989828));

        /*
         * Runtime distributions.
         */
        distributions.put("UNTAR_TIME", Distribution.getConstantDistribution(53));
        distributions.put("GUNZIP_TIME", Distribution.getTruncatedNormalDistribution(27.5, 756.25));

        distributions.put("FASTQC_TIME", Distribution.getTruncatedNormalDistribution(8.0, 64.0));
        distributions.put("FAIDX_TIME", Distribution.getTruncatedNormalDistribution(0.04, 0.0384));
        distributions.put("BUILD_TIME", Distribution.getTruncatedNormalDistribution(85.64, 6642.5504));
        distributions.put("ALIGN_TIME", Distribution.getTruncatedNormalDistribution(0.96, 0.0384));
        distributions.put("SORT_TIME", Distribution.getTruncatedNormalDistribution(2.96, 3.1584));
        distributions.put("PILEUP_TIME", Distribution.getTruncatedNormalDistribution(16.68, 227.8976));
        distributions.put("VARSCAN_TIME", Distribution.getTruncatedNormalDistribution(18.2, 184.72));
        distributions.put("ANNOVAR_TIME", Distribution.getConstantDistribution(13));

        /*
         * Memory models.
         */
        memoryModels.put("UNTAR_MEM", LinearModel.constant(6294.405120e6, 0.64e6, 10e6));
        memoryModels.put("GUNZIP_MEM", LinearModel.constant(225751040, 0.64e6, 10e6));
        memoryModels.put("FASTQC_MEM", LinearModel.constant(172609536, 0.64e6, 10e6));
        memoryModels.put("FAIDX_MEM", LinearModel.constant(1138688, 0.64e6, 10e6));
        memoryModels.put("BUILD_MEM", new LinearModel(7.06621219e+00, -2.50037354e+07, 75168825.46073712, 10e6));
        memoryModels.put("ALIGN_MEM", new LinearModel(-0.00313234721648, 8219825.53808, 1839863.20373326, 10e6));
        memoryModels.put("SORT_MEM", new LinearModel(5.98052744905, -25085974.9131, 10229878.82462673, 10e6));
        memoryModels.put("PILEUP_MEM", new LinearModel(0.98062275764, -16982382.2809, 25700732.44343742, 10e6));
        memoryModels.put("VARSCAN_MEM", new LinearModel(1.2142658008, 2376073937.43, 1.18779457e+08, 10e6));
        memoryModels.put("ANNOVAR_MEM", LinearModel.constant(470867968, 0.16e6, 10e6));

        /*
         * Peak memory relative time distributions.
         */
        Distribution peakMemRelativeTime = Distribution.getUniformDistribution(0.4,0.6);
        distributions.put("UNTAR_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("GUNZIP_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("FASTQC_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("FAIDX_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("BUILD_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("ALIGN_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("SORT_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("PILEUP_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("VARSCAN_peak_mem_relative_time", peakMemRelativeTime);
        distributions.put("ANNOVAR_peak_mem_relative_time", peakMemRelativeTime);

    }

    @Override
    public String[] getTasktypes() {
        return new String[]{"VC_Untar", "VC_Gunzip", "VC_Fastqc", "VC_Build", "VC_Faidx", "VC_Align", "VC_Sort", "VC_Pileup", "VC_Varscan", "VC_Annovar"};
    }

}

class VC_Untar extends AppJob {

    private VariantCalling vc;
    private HashMap<Integer, Long> pathFsize = new HashMap<>();

    VC_Untar(VariantCalling vc, String name, String version, String jobID) {
        super(vc, VariantCalling.NAMESPACE, name, version, jobID);

        this.vc = vc;

        long size = vc.generateLong("UNTAR_INPUT");
        input("hg38.tar", size);

        double runtime                = vc.generateDouble("UNTAR_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("UNTAR_MEM").generate(2*size);
        double peakMemoryTimeRelative = vc.generateDouble("UNTAR_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+2*size);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

    void addChild(AppJob child, int path) {

        // Check if the output file for this task has been generated
        long fileSize = 0;

        if (!pathFsize.containsKey(fileSize)) {

            fileSize = vc.generateLong("UNTAR_OUTPUT");
            pathFsize.put(path, fileSize);

        } else {

            fileSize = pathFsize.get(path);

        }

        addLink(child, "chr" + path + ".fa", fileSize);

    }

}

class VC_Gunzip extends AppJob {

    private String outFileName;
    private long outFileSize;

    VC_Gunzip(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);

        long size = vc.generateLong("GUNZIP_INPUT");
        input("SRR359188_" + jobID + ".filt.fastq.gz", size);  // 223618467  225289381

        double runtime                = vc.generateDouble("GUNZIP_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("GUNZIP_MEM").generate(2*size);
        double peakMemoryTimeRelative = vc.generateDouble("GUNZIP_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+2*size);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

        // Prdoduces only one file, that is used by all its children
        outFileName = "unzipped_SRR359188_" + jobID + ".filt.fastq.gz";
        outFileSize = vc.generateLong("GUNZIP_OUTPUT");

    }

    public void addChild(AppJob child) {

        addLink(child, outFileName, outFileSize);

    }

}

class VC_Fastqc extends AppJob {

    private VariantCalling vc;

    VC_Fastqc(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

        output("SRR359188_" + jobID + ".filt_fastqc.zip", vc.generateLong("FASTQC_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("FASTQC_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("FASTQC_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("FASTQC_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Faidx extends AppJob {

    private VariantCalling vc;

    VC_Faidx(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // Task has always one child, thus the output file will be added here
        addLink(child, "chr" + path + ".fa.fai", vc.generateLong("FAIDX_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("FAIDX_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("FAIDX_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("FAIDX_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Build extends AppJob {

    private VariantCalling vc;

    VC_Build(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // VC_Build has always one child, thus the output file will be added here
        addLink(child, path + "_idx.tar", vc.generateLong("BUILD_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("BUILD_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("BUILD_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("BUILD_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Align extends AppJob {

    private VariantCalling vc;

    VC_Align(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // Task has always one child, thus the output file will be added here
        addLink(child, path + "_alignment.bam", vc.generateLong("ALIGN_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("ALIGN_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("ALIGN_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("ALIGN_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Sort extends AppJob {

    private VariantCalling vc;

    VC_Sort(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // Task has always one child, thus the output file will be added here
        addLink(child, path + "_sorted.bam", vc.generateLong("SORT_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("SORT_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("SORT_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("SORT_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Pileup extends AppJob {

    private VariantCalling vc;

    VC_Pileup(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // Task has always one child, thus the output file will be added here
        addLink(child, path + "_mpileup.csv", vc.generateLong("PILEUP_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("PILEUP_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("PILEUP_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("PILEUP_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Varscan extends AppJob {

    private VariantCalling vc;

    VC_Varscan(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    void addChild(AppJob child, int path) {

        // Task has always one child, thus the output file will be added here
        addLink(child, path + "_variants.vcf", vc.generateLong("VARSCAN_OUTPUT"));

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        double runtime                = vc.generateDouble("VARSCAN_TIME") * vc.getRuntimeFactor();
        long   peakMemory             = vc.memoryModels.get("VARSCAN_MEM").generate(inputSize);
        double peakMemoryTimeRelative = vc.generateDouble("VARSCAN_peak_mem_relative_time");

        addAnnotation("runtime", String.format("%.2f", runtime));
        addAnnotation("input_total_bytes", ""+inputSize);
        addAnnotation("peak_mem_bytes", ""+peakMemory);

        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));

    }

}

class VC_Annovar extends AppJob {

    private VariantCalling vc;

    VC_Annovar(VariantCalling vc, String name, String version, String jobID) {

        super(vc, VariantCalling.NAMESPACE, name, version, jobID);
        this.vc = vc;

    }

    @Override
    public void finish() {

        long inputSize = 0;
        Set<AppFilename> inputs = getInputs();

        for (AppFilename input : inputs)
            inputSize += input.getSize();

        // Determine the output size
        long annovarSize = vc.generateLong("ANNOVAR_OUTPUT");
        output("table.variant_function", annovarSize);

        // Determine the runtime
        double runtime = vc.generateDouble("ANNOVAR_TIME");
        addAnnotation("runtime", String.format("%.2f", runtime));

        // Determine the peak memory consumption
        long   peakMemory             = vc.memoryModels.get("ANNOVAR_MEM").generate(annovarSize);
        double peakMemoryTimeRelative = vc.generateDouble("ANNOVAR_peak_mem_relative_time");

        addAnnotation("input_total_bytes", inputSize+"");
        addAnnotation("peak_mem_bytes", ""+peakMemory);
        addArgument(new PseudoText(String.format("peak_mem_bytes=%d,peak_memory_relative_time=%.3f", peakMemory, peakMemoryTimeRelative)));
    }
}

