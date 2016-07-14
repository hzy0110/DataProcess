package com.hzy.util;



import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.text.MultipleTextFileInputFormat;
import org.apache.mahout.text.PrefixAdditionFilter;
import org.apache.mahout.text.SequenceFilesFromDirectoryFilter;
import org.apache.mahout.text.SequenceFilesFromDirectoryMapper;
import org.apache.mahout.utils.io.ChunkedWriter;
/**
 * Created by zy on 2016/7/14.
 */
public class SequenceFilesFromDirectory extends AbstractJob {
    private static final String PREFIX_ADDITION_FILTER = PrefixAdditionFilter.class.getName();
    private static final String[] CHUNK_SIZE_OPTION = new String[]{"chunkSize", "chunk"};
    public static final String[] FILE_FILTER_CLASS_OPTION = new String[]{"fileFilterClass", "filter"};
    private static final String[] CHARSET_OPTION = new String[]{"charset", "c"};
    private static final int MAX_JOB_SPLIT_LOCATIONS = 1000000;
    public static final String[] KEY_PREFIX_OPTION = new String[]{"keyPrefix", "prefix"};
    public static final String BASE_INPUT_PATH = "baseinputpath";

    public SequenceFilesFromDirectory() {
        setConf(HUtils.getConf());
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SequenceFilesFromDirectory(), args);
    }

    public int run(String[] args) throws Exception {
        this.addOptions();
        this.addOption(DefaultOptionCreator.methodOption().create());
        this.addOption(DefaultOptionCreator.overwriteOption().create());
        if(this.parseArguments(args) == null) {
            return -1;
        } else {
            Map options = this.parseOptions();
            Path output = this.getOutputPath();
            if(this.hasOption("overwrite")) {
                HadoopUtil.delete(HUtils.getConf(), new Path[]{output});
            }

            if(this.getOption("method", "mapreduce").equals("sequential")) {
                this.runSequential(HUtils.getConf(), this.getInputPath(), output, options);
            } else {
                this.runMapReduce(this.getInputPath(), output);
            }

            return 0;
        }
    }

    private int runSequential(Configuration conf, Path input, Path output, Map<String, String> options) throws IOException, InterruptedException, NoSuchMethodException {
        Charset charset = Charset.forName(this.getOption(CHARSET_OPTION[0]));
        String keyPrefix = this.getOption(KEY_PREFIX_OPTION[0]);
        FileSystem fs = FileSystem.get(input.toUri(), conf);
        ChunkedWriter writer = new ChunkedWriter(conf, Integer.parseInt((String)options.get(CHUNK_SIZE_OPTION[0])), output);
        Throwable var9 = null;

        try {
            String fileFilterClassName = (String)options.get(FILE_FILTER_CLASS_OPTION[0]);
            Object x2;
            if(PrefixAdditionFilter.class.getName().equals(fileFilterClassName)) {
                x2 = new PrefixAdditionFilter(conf, keyPrefix, options, writer, charset, fs);
            } else {
                x2 = (SequenceFilesFromDirectoryFilter)ClassUtils.instantiateAs(fileFilterClassName, SequenceFilesFromDirectoryFilter.class, new Class[]{Configuration.class, String.class, Map.class, ChunkedWriter.class, Charset.class, FileSystem.class}, new Object[]{conf, keyPrefix, options, writer, charset, fs});
            }

            fs.listStatus(input, (PathFilter)x2);
        } catch (Throwable var19) {
            var9 = var19;
            throw var19;
        } finally {
            if(writer != null) {
                if(var9 != null) {
                    try {
                        writer.close();
                    } catch (Throwable var18) {
                        var9.addSuppressed(var18);
                    }
                } else {
                    writer.close();
                }
            }

        }

        return 0;
    }

    private int runMapReduce(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
        int chunkSizeInMB = 64;
        if(this.hasOption(CHUNK_SIZE_OPTION[0])) {
            chunkSizeInMB = Integer.parseInt(this.getOption(CHUNK_SIZE_OPTION[0]));
        }

        String keyPrefix = null;
        if(this.hasOption(KEY_PREFIX_OPTION[0])) {
            keyPrefix = this.getOption(KEY_PREFIX_OPTION[0]);
        }

        String fileFilterClassName = null;
        if(this.hasOption(FILE_FILTER_CLASS_OPTION[0])) {
            fileFilterClassName = this.getOption(FILE_FILTER_CLASS_OPTION[0]);
        }

        PathFilter pathFilter = null;
        if(!StringUtils.isBlank(fileFilterClassName) && !PrefixAdditionFilter.class.getName().equals(fileFilterClassName)) {
            try {
                pathFilter = (PathFilter)Class.forName(fileFilterClassName).newInstance();
            } catch (IllegalAccessException | InstantiationException var15) {
                throw new IllegalStateException(var15);
            }
        }

        Job job = this.prepareJob(input, output, MultipleTextFileInputFormat.class, SequenceFilesFromDirectoryMapper.class, Text.class, Text.class, SequenceFileOutputFormat.class, "SequenceFilesFromDirectory");
        Configuration jobConfig = job.getConfiguration();
        jobConfig.set(KEY_PREFIX_OPTION[0], keyPrefix);
        jobConfig.set(FILE_FILTER_CLASS_OPTION[0], fileFilterClassName);
        FileSystem fs = FileSystem.get(jobConfig);
        FileStatus fsFileStatus = fs.getFileStatus(input);
        String inputDirList;
        if(pathFilter != null) {
            inputDirList = HadoopUtil.buildDirList(fs, fsFileStatus, pathFilter);
        } else {
            inputDirList = HadoopUtil.buildDirList(fs, fsFileStatus);
        }

        jobConfig.set("baseinputpath", input.toString());
        long chunkSizeInBytes = (long)(chunkSizeInMB * 1024 * 1024);
        jobConfig.set("mapreduce.job.max.split.locations", String.valueOf(1000000));
        FileInputFormat.setInputPaths(job, inputDirList);
        FileInputFormat.setMaxInputSplitSize(job, chunkSizeInBytes);
        FileOutputFormat.setCompressOutput(job, true);
        boolean succeeded = job.waitForCompletion(true);
        return !succeeded?-1:0;
    }

    protected void addOptions() {
        this.addInputOption();
        this.addOutputOption();
        this.addOption(DefaultOptionCreator.overwriteOption().create());
        this.addOption(DefaultOptionCreator.methodOption().create());
        this.addOption(CHUNK_SIZE_OPTION[0], CHUNK_SIZE_OPTION[1], "The chunkSize in MegaBytes. Defaults to 64", "64");
        this.addOption(FILE_FILTER_CLASS_OPTION[0], FILE_FILTER_CLASS_OPTION[1], "The name of the class to use for file parsing. Default: " + PREFIX_ADDITION_FILTER, PREFIX_ADDITION_FILTER);
        this.addOption(KEY_PREFIX_OPTION[0], KEY_PREFIX_OPTION[1], "The prefix to be prepended to the key", "");
        this.addOption(CHARSET_OPTION[0], CHARSET_OPTION[1], "The name of the character encoding of the input files. Default to UTF-8", "UTF-8");
    }

    protected Map<String, String> parseOptions() {
        HashMap options = new HashMap();
        options.put(CHUNK_SIZE_OPTION[0], this.getOption(CHUNK_SIZE_OPTION[0]));
        options.put(FILE_FILTER_CLASS_OPTION[0], this.getOption(FILE_FILTER_CLASS_OPTION[0]));
        options.put(CHARSET_OPTION[0], this.getOption(CHARSET_OPTION[0]));
        return options;
    }
}