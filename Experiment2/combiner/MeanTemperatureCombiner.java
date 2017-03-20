
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import org.apache.log4j.BasicConfigurator;


/***
 * MeanTemperatureCombiner is the driver class for the mapreduce program
 */
public class MeanTemperatureCombiner
{
    /***
     * Main : Setups up the mapreduce environment configuration.
     * This program involves Mapper,Combiner and Reducer. The Map output key is stationID, output value is of type StationTempInfo
     * Input file name and output directory configuration is read from args
     * @param args : contains two parameters input file name and the output directory name.
     */
    public static void main( String[] args )
    {

        try {
            Configuration conf = new Configuration();
            BasicConfigurator.configure();
            Job job = Job.getInstance(conf, "Mean Temperature");
            job.setJarByClass(MeanTemperatureCombiner.class);
            job.setMapperClass(MeanTempMapper.class);
            job.setCombinerClass(MeanTempCombiner.class);
            job.setReducerClass(MeanTempReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(StationTempInfo.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

/***
 * StationTempInfo: Value type class. Object of this class is passed as the output value from the mapper.
 * Contains aggregate maxtemp,mintemp, the counts for each of those.
 */
class StationTempInfo implements Writable{
    public int maxtemp;
    public int mintemp;
    public long maxCnt;
    public long minCnt;

    public StationTempInfo()
    {

    }
    public StationTempInfo(int maxtemp,int mintemp,long maxCnt,long minCnt){
        this.maxtemp=maxtemp;
        this.mintemp=mintemp;
        this.maxCnt=maxCnt;
        this.minCnt=minCnt;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(maxtemp);
        dataOutput.writeInt(mintemp);
        dataOutput.writeLong(maxCnt);
        dataOutput.writeLong(minCnt);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        maxtemp=dataInput.readInt();
        mintemp=dataInput.readInt();
        maxCnt=dataInput.readLong();
        minCnt=dataInput.readLong();
    }
}

/***
 * MeanTempMapper: Mapper class which contains map function
 * which produces stationID,StationTempInfo as the key,value
 */
class MeanTempMapper extends Mapper<Object,Text,Text,StationTempInfo>{
    /***
     * map : The input station record information is split by ',' to fetch individual fields. StationTempInfo object is created
     * to which the current record's maxtemp or mintemp is added with other two fields set as 0
     * The same is emitted from mapper using context.
     * @param key : Input key to mapper.
     * @param value : contains the line from the input file that has the station temperature information.
     * @param context :Used to emit output from Mapper
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        String[] StationDetails;
        StationDetails=value.toString().split(",");
        Text stationID=new Text();
        stationID.set(StationDetails[0]);
        if(StationDetails.length >=4 && StationDetails[2].equals("TMIN")){
            int TMinVal = Integer.parseInt(StationDetails[3]);
            StationTempInfo station_info=new StationTempInfo(0,TMinVal,0,1);
            context.write(stationID,station_info);
        }
        else if(StationDetails.length >=4 && StationDetails[2].equals("TMAX")){
            int TMaxVal = Integer.parseInt(StationDetails[3]);
            StationTempInfo station_info=new StationTempInfo(TMaxVal,0,1,0);
            context.write(stationID,station_info);

        }
    }
}

/***
 * MeanTempCombiner : Combines multiple temperature records for same stationID into a single record with the
 * data aggregated.
 */
class MeanTempCombiner extends Reducer<Text,StationTempInfo,Text,StationTempInfo> {
    /***
     * combines the list of stationinfos for the key stationID into a single object with aggregated content.
     * @param key - stationID
     * @param stationTempInfos - list of stationinfo for the key stationID
     * @param context : used to emit records from combiner
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<StationTempInfo> stationTempInfos, Context context) throws IOException,InterruptedException{
        StationTempInfo combined_stationInfo = new StationTempInfo();
        for(StationTempInfo stationTempInfo:stationTempInfos){
            combined_stationInfo.maxtemp+=stationTempInfo.maxtemp;
            combined_stationInfo.mintemp+=stationTempInfo.mintemp;
            combined_stationInfo.maxCnt+=stationTempInfo.maxCnt;
            combined_stationInfo.minCnt+=stationTempInfo.minCnt;
        }
        context.write(key,combined_stationInfo);
    }
}

/***
 * MeanTempReducer : Reduce task is created per stationID
 */
class MeanTempReducer extends Reducer<Text,StationTempInfo,NullWritable,Text> {
    /***
     * reduce: The mean max and mean min temperatures for the incoming key(stationID) is aggregated in loop and the corresponding average is generated
     * as output from the reducer.
     * @param key : stationID
     * @param stationTempInfos: Contains the list of stationTempInfo records which contains the given stationIDs temperature records.
     * @param context: Used to emit output from reducer
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<StationTempInfo> stationTempInfos, Context context) throws IOException,InterruptedException{
        double tMaxAvg=0.0;
        double tMinAvg=0.0;
        long tMaxCnt=0;
        long tMinCnt=0;
        String result=key+", ";
        for(StationTempInfo stationTempInfo:stationTempInfos){
            tMaxAvg+=stationTempInfo.maxtemp;
            tMinAvg+=stationTempInfo.mintemp;
            tMaxCnt+=stationTempInfo.maxCnt;
            tMinCnt+=stationTempInfo.minCnt;
        }
        if(tMinCnt>0){
            tMinAvg=tMinAvg/tMinCnt;
            result+=Double.toString(tMinAvg)+", ";
        }
        else{
            result+="null, ";
        }

        if(tMaxCnt>0){
            tMaxAvg=tMaxAvg/tMaxCnt;
            result+=Double.toString(tMaxAvg);
        }
        else{
            result+="null";
        }
        context.write(NullWritable.get(),new Text(result));
    }
}