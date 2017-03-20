import java.io.IOException;


import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
/**
 * Created by vikasjanardhanan on 2/20/17.
 */
//Driver Class
public class PageRank {

    static String TOTALPAGES = "TotalPages";
    static String DELTA = "Delta";
    static String ISFIRSTRUN = "isFirstRun";

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
            long preProcessEndTime=System.currentTimeMillis();
            double delta=0.0;
            long execTime=preProcessEndTime-preProcessStartTime;
            System.out.println("Preprocessing completed in "+execTime+" ms");
            //End preprocessing task
            // Start PageRank computation for 10 runs
            for(int i=0;i<10;i++){
                System.out.println("Starting iteration "+i+" for pagerank calculation");
                long pageRankStartTime=System.currentTimeMillis();
                Job pageRankJob=createPageRankJob(i,remainingArgs,conf,totalPages,delta);
                pageRankJob.waitForCompletion(true);
                delta=Double.longBitsToDouble(pageRankJob.getCounters().findCounter(counter.delta).getValue());
                long pageRankEndTime=System.currentTimeMillis();
                execTime=pageRankEndTime-pageRankStartTime;
                System.out.println("Page Rank computation completed in "+execTime+" ms");

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
        job.setJarByClass(PageRank.class);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("temp/PageRank10"));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length-1]));
        return job;
    }

    public static Job createpreprocessJob(String[] remainingArgs,Configuration conf) throws IOException{

        Job job = Job.getInstance(conf, "Pre Process");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Bz2ParserMapper.class);
        job.setCombinerClass(Bz2ParserCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageNode.class);
        job.setReducerClass(Bz2ParserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageNode.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        for(int i=0;i<remainingArgs.length-1;i++) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path("temp/PageRank0"));
        return job;
    }

    public static Job createPageRankJob(int iterCount,String[] remainingArgs,Configuration conf,long totalPages,double delta) throws IOException{

        conf.setLong(TOTALPAGES,totalPages);
        if(iterCount==0){
            conf.setBoolean(ISFIRSTRUN,true);
        }
        else{
            conf.setBoolean(ISFIRSTRUN,false);
        }
        conf.setDouble(DELTA,delta);
        Job job = Job.getInstance(conf, "Page Rank Calculator");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setCombinerClass(PageRankCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageNode.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageNode.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("temp/PageRank"+iterCount));
        FileOutputFormat.setOutputPath(job, new Path("temp/PageRank"+(iterCount+1)));
        return job;
    }
}
