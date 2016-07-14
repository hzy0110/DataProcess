package com.hzy.util;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.option.DefaultOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.lucene.AnalyzerUtils;
import org.apache.mahout.math.hadoop.stats.BasicStats;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Hzy on 2016/7/14.
 */
public class SparseVectorsFromSequenceFiles extends AbstractJob {
    private static final Logger log = LoggerFactory.getLogger(SparseVectorsFromSequenceFiles.class);

    public SparseVectorsFromSequenceFiles() {
        setConf(HUtils.getConf());
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SparseVectorsFromSequenceFiles(), args);
    }

    public int run(String[] args) throws Exception {
        DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
        ArgumentBuilder abuilder = new ArgumentBuilder();
        GroupBuilder gbuilder = new GroupBuilder();
        DefaultOption inputDirOpt = DefaultOptionCreator.inputOption().create();
        DefaultOption outputDirOpt = DefaultOptionCreator.outputOption().create();
        DefaultOption minSupportOpt = obuilder.withLongName("minSupport").withArgument(abuilder.withName("minSupport").withMinimum(1).withMaximum(1).create()).withDescription("(Optional) Minimum Support. Default Value: 2").withShortName("s").create();
        DefaultOption analyzerNameOpt = obuilder.withLongName("analyzerName").withArgument(abuilder.withName("analyzerName").withMinimum(1).withMaximum(1).create()).withDescription("The class name of the analyzer").withShortName("a").create();
        DefaultOption chunkSizeOpt = obuilder.withLongName("chunkSize").withArgument(abuilder.withName("chunkSize").withMinimum(1).withMaximum(1).create()).withDescription("The chunkSize in MegaBytes. Default Value: 100MB").withShortName("chunk").create();
        DefaultOption weightOpt = obuilder.withLongName("weight").withRequired(false).withArgument(abuilder.withName("weight").withMinimum(1).withMaximum(1).create()).withDescription("The kind of weight to use. Currently TF or TFIDF. Default: TFIDF").withShortName("wt").create();
        DefaultOption minDFOpt = obuilder.withLongName("minDF").withRequired(false).withArgument(abuilder.withName("minDF").withMinimum(1).withMaximum(1).create()).withDescription("The minimum document frequency.  Default is 1").withShortName("md").create();
        DefaultOption maxDFPercentOpt = obuilder.withLongName("maxDFPercent").withRequired(false).withArgument(abuilder.withName("maxDFPercent").withMinimum(1).withMaximum(1).create()).withDescription("The max percentage of docs for the DF.  Can be used to remove really high frequency terms. Expressed as an integer between 0 and 100. Default is 99.  If maxDFSigma is also set, it will override this value.").withShortName("x").create();
        DefaultOption maxDFSigmaOpt = obuilder.withLongName("maxDFSigma").withRequired(false).withArgument(abuilder.withName("maxDFSigma").withMinimum(1).withMaximum(1).create()).withDescription("What portion of the tf (tf-idf) vectors to be used, expressed in times the standard deviation (sigma) of the document frequencies of these vectors. Can be used to remove really high frequency terms. Expressed as a double value. Good value to be specified is 3.0. In case the value is less than 0 no vectors will be filtered out. Default is -1.0.  Overrides maxDFPercent").withShortName("xs").create();
        DefaultOption minLLROpt = obuilder.withLongName("minLLR").withRequired(false).withArgument(abuilder.withName("minLLR").withMinimum(1).withMaximum(1).create()).withDescription("(Optional)The minimum Log Likelihood Ratio(Float)  Default is 1.0").withShortName("ml").create();
        DefaultOption numReduceTasksOpt = obuilder.withLongName("numReducers").withArgument(abuilder.withName("numReducers").withMinimum(1).withMaximum(1).create()).withDescription("(Optional) Number of reduce tasks. Default Value: 1").withShortName("nr").create();
        DefaultOption powerOpt = obuilder.withLongName("norm").withRequired(false).withArgument(abuilder.withName("norm").withMinimum(1).withMaximum(1).create()).withDescription("The norm to use, expressed as either a float or \"INF\" if you want to use the Infinite norm.  Must be greater or equal to 0.  The default is not to normalize").withShortName("n").create();
        DefaultOption logNormalizeOpt = obuilder.withLongName("logNormalize").withRequired(false).withDescription("(Optional) Whether output vectors should be logNormalize. If set true else false").withShortName("lnorm").create();
        DefaultOption maxNGramSizeOpt = obuilder.withLongName("maxNGramSize").withRequired(false).withArgument(abuilder.withName("ngramSize").withMinimum(1).withMaximum(1).create()).withDescription("(Optional) The maximum size of ngrams to create (2 = bigrams, 3 = trigrams, etc) Default Value:1").withShortName("ng").create();
        DefaultOption sequentialAccessVectorOpt = obuilder.withLongName("sequentialAccessVector").withRequired(false).withDescription("(Optional) Whether output vectors should be SequentialAccessVectors. If set true else false").withShortName("seq").create();
        DefaultOption namedVectorOpt = obuilder.withLongName("namedVector").withRequired(false).withDescription("(Optional) Whether output vectors should be NamedVectors. If set true else false").withShortName("nv").create();
        DefaultOption overwriteOutput = obuilder.withLongName("overwrite").withRequired(false).withDescription("If set, overwrite the output directory").withShortName("ow").create();
        DefaultOption helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h").create();
        Group group = gbuilder.withName("Options").withOption(minSupportOpt).withOption(analyzerNameOpt).withOption(chunkSizeOpt).withOption(outputDirOpt).withOption(inputDirOpt).withOption(minDFOpt).withOption(maxDFSigmaOpt).withOption(maxDFPercentOpt).withOption(weightOpt).withOption(powerOpt).withOption(minLLROpt).withOption(numReduceTasksOpt).withOption(maxNGramSizeOpt).withOption(overwriteOutput).withOption(helpOpt).withOption(sequentialAccessVectorOpt).withOption(namedVectorOpt).withOption(logNormalizeOpt).create();

        try {
            Parser e = new Parser();
            e.setGroup(group);
            e.setHelpOption(helpOpt);
            CommandLine cmdLine = e.parse(args);
            if(cmdLine.hasOption(helpOpt)) {
                CommandLineUtil.printHelp(group);
                return -1;
            }

            Path inputDir = new Path((String)cmdLine.getValue(inputDirOpt));
            Path outputDir = new Path((String)cmdLine.getValue(outputDirOpt));
            int chunkSize = 100;
            if(cmdLine.hasOption(chunkSizeOpt)) {
                chunkSize = Integer.parseInt((String)cmdLine.getValue(chunkSizeOpt));
            }

            int minSupport = 2;
            if(cmdLine.hasOption(minSupportOpt)) {
                String maxNGramSize = (String)cmdLine.getValue(minSupportOpt);
                minSupport = Integer.parseInt(maxNGramSize);
            }

            int maxNGramSize1 = 1;
            if(cmdLine.hasOption(maxNGramSizeOpt)) {
                try {
                    maxNGramSize1 = Integer.parseInt(cmdLine.getValue(maxNGramSizeOpt).toString());
                } catch (NumberFormatException var57) {
                    log.warn("Could not parse ngram size option");
                }
            }

            log.info("Maximum n-gram size is: {}", Integer.valueOf(maxNGramSize1));
            if(cmdLine.hasOption(overwriteOutput)) {
                HadoopUtil.delete(HUtils.getConf(), new Path[]{outputDir});
            }

            float minLLRValue = 1.0F;
            if(cmdLine.hasOption(minLLROpt)) {
                minLLRValue = Float.parseFloat(cmdLine.getValue(minLLROpt).toString());
            }

            log.info("Minimum LLR value: {}", Float.valueOf(minLLRValue));
            int reduceTasks = 1;
            if(cmdLine.hasOption(numReduceTasksOpt)) {
                reduceTasks = Integer.parseInt(cmdLine.getValue(numReduceTasksOpt).toString());
            }

            log.info("Number of reduce tasks: {}", Integer.valueOf(reduceTasks));
            Class analyzerClass = StandardAnalyzer.class;
            if(cmdLine.hasOption(analyzerNameOpt)) {
                String processIdf = cmdLine.getValue(analyzerNameOpt).toString();
                analyzerClass = Class.forName(processIdf).asSubclass(Analyzer.class);
                AnalyzerUtils.createAnalyzer(analyzerClass);
            }

            boolean processIdf1;
            if(cmdLine.hasOption(weightOpt)) {
                String minDf = cmdLine.getValue(weightOpt).toString();
                if("tf".equalsIgnoreCase(minDf)) {
                    processIdf1 = false;
                } else {
                    if(!"tfidf".equalsIgnoreCase(minDf)) {
                        throw new OptionException(weightOpt);
                    }

                    processIdf1 = true;
                }
            } else {
                processIdf1 = true;
            }

            int minDf1 = 1;
            if(cmdLine.hasOption(minDFOpt)) {
                minDf1 = Integer.parseInt(cmdLine.getValue(minDFOpt).toString());
            }

            int maxDFPercent = 99;
            if(cmdLine.hasOption(maxDFPercentOpt)) {
                maxDFPercent = Integer.parseInt(cmdLine.getValue(maxDFPercentOpt).toString());
            }

            double maxDFSigma = -1.0D;
            if(cmdLine.hasOption(maxDFSigmaOpt)) {
                maxDFSigma = Double.parseDouble(cmdLine.getValue(maxDFSigmaOpt).toString());
            }

            float norm = -1.0F;
            if(cmdLine.hasOption(powerOpt)) {
                String logNormalize = cmdLine.getValue(powerOpt).toString();
                if("INF".equals(logNormalize)) {
                    norm = 1.0F / 0.0F;
                } else {
                    norm = Float.parseFloat(logNormalize);
                }
            }

            boolean logNormalize1 = false;
            if(cmdLine.hasOption(logNormalizeOpt)) {
                logNormalize1 = true;
            }

            log.info("Tokenizing documents in {}", inputDir);
            Configuration conf = getConf();
            Path tokenizedPath = new Path(outputDir, "tokenized-documents");
            DocumentProcessor.tokenizeDocuments(inputDir, analyzerClass, tokenizedPath, conf);
            boolean sequentialAccessOutput = false;
            if(cmdLine.hasOption(sequentialAccessVectorOpt)) {
                sequentialAccessOutput = true;
            }

            boolean namedVectors = false;
            if(cmdLine.hasOption(namedVectorOpt)) {
                namedVectors = true;
            }

            boolean shouldPrune = maxDFSigma >= 0.0D || (double)maxDFPercent > 0.0D;
            String tfDirName = shouldPrune?"tf-vectors-toprune":"tf-vectors";
            log.info("Creating Term Frequency Vectors");
            if(processIdf1) {
                DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDir, tfDirName, conf, minSupport, maxNGramSize1, minLLRValue, -1.0F, false, reduceTasks, chunkSize, sequentialAccessOutput, namedVectors);
            } else {
                DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDir, tfDirName, conf, minSupport, maxNGramSize1, minLLRValue, norm, logNormalize1, reduceTasks, chunkSize, sequentialAccessOutput, namedVectors);
            }

            Pair docFrequenciesFeatures = null;
            if(shouldPrune || processIdf1) {
                log.info("Calculating IDF");
                docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDir, tfDirName), outputDir, conf, chunkSize);
            }

            long maxDF = (long)maxDFPercent;
            if(shouldPrune) {
                long vectorCount = ((Long[])docFrequenciesFeatures.getFirst())[1].longValue();
                if(maxDFSigma >= 0.0D) {
                    Path maxDFThreshold = new Path(outputDir, "df-count");
                    Path stdCalcDir = new Path(outputDir, "stdcalc");
                    double tfDir = BasicStats.stdDevForGivenMean(maxDFThreshold, stdCalcDir, 0.0D, conf);
                    maxDF = (long)((int)(100.0D * maxDFSigma * tfDir / (double)vectorCount));
                }

                long maxDFThreshold1 = (long)((float)vectorCount * ((float)maxDF / 100.0F));
                Path tfDir1 = new Path(outputDir, tfDirName);
                Path prunedTFDir = new Path(outputDir, "tf-vectors");
                Path prunedPartialTFDir = new Path(outputDir, "tf-vectors-partial");
                log.info("Pruning");
                if(processIdf1) {
                    HighDFWordsPruner.pruneVectors(tfDir1, prunedTFDir, prunedPartialTFDir, maxDFThreshold1, (long)minDf1, conf, docFrequenciesFeatures, -1.0F, false, reduceTasks);
                } else {
                    HighDFWordsPruner.pruneVectors(tfDir1, prunedTFDir, prunedPartialTFDir, maxDFThreshold1, (long)minDf1, conf, docFrequenciesFeatures, norm, logNormalize1, reduceTasks);
                }

                HadoopUtil.delete(new Configuration(conf), new Path[]{tfDir1});
            }

            if(processIdf1) {
                TFIDFConverter.processTfIdf(new Path(outputDir, "tf-vectors"), outputDir, conf, docFrequenciesFeatures, minDf1, maxDF, norm, logNormalize1, sequentialAccessOutput, namedVectors, reduceTasks);
            }
        } catch (OptionException var58) {
            log.error("Exception", var58);
            CommandLineUtil.printHelp(group);
        }

        return 0;
    }
}
