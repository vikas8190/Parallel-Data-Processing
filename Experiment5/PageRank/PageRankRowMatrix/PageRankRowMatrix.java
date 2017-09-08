import java.io.IOException;


import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.net.URI;
import java.net.URISyntaxException;
/**
 * Created by vikasjanardhanan on 2/20/17.
 */
//Driver Class
public class PageRankRowMatrix {

    static String TOTALPAGES = "TotalPages";
    static String DELTA = "Delta";
    static String ISFIRSTRUN = "isFirstRun";

    static String IterationCount = "IterNumber";
    static String DanglingContrib = "Dangling";

    static String MatrixFileID = "Matrix";
    static String NumOffsetFileID = "FileNumOffset";

    static String DanglingNodeFileID = "Dangling";

    static String NumberPageNameMapFileID = "Map";// Number to
    static String PageNumbersFileID = "PageNumbers";

    static String MatrixPathPrefix = "matrix/matrix";
    static String NumberPageNameMapPathPrefix = "map/map";
    static String PageNumberPathPrefix = "PageNumber/PageNumber";
    static String DanglingPathPrefix = "dangling/dangling";
    static String OffsetFilePathPrefix = "offset/offset";


    //StringToNum
    static String StringToNumberMapFileID = "StringToNumMap";
    static String StringToNumberOffsetID = "Offset";
    static String StringToNumberFilePathPrefix = "stringToNumbermap/map";
    static String OffsetSubDir = "/offset";
    static String stringToMapInput = "/stringToNumbermap";

    //Adjacency
    static String AdjListFileID = "Adjacency";
    static String AdjListFilePathPrefix = "adjacency/adjacency";
    static String AdjListSubDir = "/adjacency";

    static String PrePrcoessOutputDir = "temp/output";
    static String PageNameToNumberOutputDir = "outputPageNameToNumber";

    static String NumberToStringID = "numberToString";
    static String NumberToStringPathPrefix = "numberToString/map";
    static String NumberToStringDir = "/numberToString";


    static String PageRank = "PageRank";
    static String MatrixSubDir = "/matrix";
    static String DanglingNodeSubDir = "/dangling";

    static String SpareMatrixOutputDir = "outputMatrix";

    // Global counter to keep track of total pages and the delta from run i-1
    static enum counter{
        TotalPages,
        delta;
    }
    public static void main(String args[]) throws IOException{

        try {
            Configuration conf = new Configuration();
            BasicConfigurator.configure();
            GenericOptionsParser optionsParser = new GenericOptionsParser(conf,args);
            String[] remainingArgs = optionsParser.getRemainingArgs();
            if (remainingArgs.length < 2){
                System.err.println("Usage: hadoop jar secondarysort.jar <in> [<in>...] <out>");
                System.exit(2);
            }

            // Start preprecessing task
            long preProcessStartTime=System.currentTimeMillis();
            Job preProcessJob = createpreprocessJob(remainingArgs,conf);
            preProcessJob.waitForCompletion(true);
            long totalPages=preProcessJob.getCounters().findCounter(counter.TotalPages).getValue();
            conf.setLong(TOTALPAGES,totalPages);

            Job convpageNameToNum = createPreProcessPageNameToNumJob(conf,totalPages);
            convpageNameToNum.waitForCompletion(true);

            Job createSparseMatrix = createSparseMatrix(conf);
            createSparseMatrix.waitForCompletion(true);

            long preProcessEndTime=System.currentTimeMillis();

            long execTime=preProcessEndTime-preProcessStartTime;
            System.out.println("Preprocessing steps 1 & 2 for creating Matrix completed in "+execTime+" ms");

            double delta=0.0;
            //End preprocessing task
            // Start PageRank computation for 10 runs
            for(int i=0;i<10;i++){
                System.out.println("Starting iteration "+i+" for pagerank calculation");
                long pageRankStartTime=System.currentTimeMillis();
                conf.setInt(IterationCount,i);

                Job matrixMultiplyJob=createMatrixMultiplyJob(conf,i);
                matrixMultiplyJob.waitForCompletion(true);
                delta=Double.longBitsToDouble(matrixMultiplyJob.getCounters().findCounter(counter.delta).getValue());
                conf.setDouble(DanglingContrib,delta);
                long pageRankEndTime=System.currentTimeMillis();
                execTime=pageRankEndTime-pageRankStartTime;
                System.out.println("Page Rank computation completed in "+execTime+" ms for iteration :"+i);

            }

            // End of pagerank computation
            //Top K job start

            long topKStartTime=System.currentTimeMillis();
            Job topKJob=createTopKJob(remainingArgs,conf);
            topKJob.waitForCompletion(true);
            long topKEndTime=System.currentTimeMillis();
            long topKExecTime=topKEndTime-topKStartTime;
            System.out.println("Top 100 computation completed in "+topKExecTime+" ms");
            //Top k end
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static Job createTopKJob(String[] remainingArgs,Configuration conf) throws IOException{

        Job job=Job.getInstance(conf, "Top K PageRank");
        job.addCacheFile(new Path(PageNameToNumberOutputDir +  NumberToStringDir).toUri());
        job.setJarByClass(PageRankRowMatrix.class);
        job.setMapperClass(TopKMapper.class);
        //job.setMapperClass(TopKMapper_working.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);
        //job.setMapOutputKeyClass(LongWritable.class);
        //job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(TopKReducer.class);
        //job.setReducerClass(TopKReducer_working.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("temp/output9"));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length-1]));
        return job;
    }

    public static Job createpreprocessJob(String[] remainingArgs,Configuration conf) throws IOException{

        Job job = Job.getInstance(conf, "Pre Process");
        job.setJarByClass(PageRankRowMatrix.class);
        job.setMapperClass(Bz2ParserMapper.class);
        //job.setCombinerClass(Bz2ParserCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageNodeValue.class);
        job.setReducerClass(Bz2ParserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageNodeValue.class);

        MultipleOutputs.addNamedOutput(job,NumOffsetFileID,SequenceFileOutputFormat.class,IntWritable.class,LongWritable.class);

        MultipleOutputs.addNamedOutput(job,AdjListFileID,SequenceFileOutputFormat.class,Text.class,PageNodeValue.class);
        MultipleOutputs.addNamedOutput(job,StringToNumberMapFileID,SequenceFileOutputFormat.class,IntWritable.class,MapWritable.class);

        for(int i=0;i<remainingArgs.length-1;i++) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path("temp/output"));
        return job;
    }


    public static Job createPreProcessPageNameToNumJob (Configuration conf, long totalPages) throws IllegalArgumentException, IOException, URISyntaxException{

        Job job= Job.getInstance(conf,"PageNameToNumber Job");
        job.addCacheFile(new Path(PrePrcoessOutputDir +  OffsetSubDir).toUri());
        job.setJarByClass(PageRankRowMatrix.class);
        job.setMapperClass(PageNameToNumberMapper.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(PrePrcoessOutputDir + stringToMapInput));
        FileOutputFormat.setOutputPath(job, new Path(PageNameToNumberOutputDir));

        MultipleOutputs.addNamedOutput(job, NumberToStringID, SequenceFileOutputFormat.class, NullWritable.class, MapWritable.class);

        MultipleOutputs.addNamedOutput(job, "dummy", SequenceFileOutputFormat.class, NullWritable.class, MapWritable.class);

        return job;

    }

    public static Job createSparseMatrix(Configuration conf) throws IOException, URISyntaxException{

        Job job = Job.getInstance(conf,"Create SparseMatrix Job");
        job.addCacheFile(new Path(PageNameToNumberOutputDir  +  stringToMapInput).toUri());

        job.setJarByClass(PageRankRowMatrix.class);
        job.setReducerClass(SparseRowMatrixReducer.class);
        job.setMapperClass(SparseRowMatrixMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ColumnContrib.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ColumnContribWritableList.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, DanglingNodeFileID, SequenceFileOutputFormat.class, NullWritable.class, LongWritable.class);

        MultipleOutputs.addNamedOutput(job, MatrixFileID, SequenceFileOutputFormat.class, LongWritable.class, ColumnContribWritableList.class);
        FileInputFormat.addInputPath(job, new Path(PrePrcoessOutputDir + AdjListSubDir));
        FileOutputFormat.setOutputPath(job, new Path(SpareMatrixOutputDir));
        return job;
    }


    public static Job createMatrixMultiplyJob(Configuration conf, int iteration) throws IOException, URISyntaxException{
        Job job = Job.getInstance(conf,"Multiply matrix job");

        job.addCacheFile(new Path(SpareMatrixOutputDir +  "/dangling").toUri());
        if(iteration > 0){
            job.addCacheFile(new Path( PrePrcoessOutputDir + (iteration-1)).toUri());
        }
        job.setJarByClass(PageRankRowMatrix.class);
        job.setMapperClass(SparseMatrixMultiplyMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(SpareMatrixOutputDir + MatrixSubDir));
        FileOutputFormat.setOutputPath(job, new Path(PrePrcoessOutputDir + iteration));
        return job;
    }

}
