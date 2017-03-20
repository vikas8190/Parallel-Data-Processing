import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import org.apache.log4j.BasicConfigurator;


/***
 * MeanTemperatureNoCombiner is the driver class for the mapreduce program
 */
public class MeanTemperatureNoCombiner
{

    /***
     * Main : Setups up the mapreduce environment configuration.
     * This program involves only Mapper and Reducer. The Map output key is stationID, output value is of type StationTempInfo
     * Input file name and output directory configuration is read from args
     * @param args : contains two parameters input file name and the output directory name.
     */
    public static void main( String[] args )
    {

        try {
            Configuration conf = new Configuration();
            BasicConfigurator.configure();
            Job job = Job.getInstance(conf, "Mean Temperature No Combiner");
            job.setJarByClass(MeanTemperatureNoCombiner.class);
            job.setMapperClass(MeanTempMapper.class);
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
 * Contains flag to indicate if the record is a max or min temperature record and the value of the
 * temperature itself.
 */
class StationTempInfo implements Writable{
    public boolean isMaxRecord;
    public int temp;

    public StationTempInfo()
    {

    }
    public StationTempInfo(boolean isMaxRecord,int temp){
        this.isMaxRecord=isMaxRecord;
        this.temp=temp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeBoolean(isMaxRecord);
        dataOutput.writeInt(temp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        isMaxRecord=dataInput.readBoolean();
        temp=dataInput.readInt();
    }
}

/***
 * MeanTempMapper: Mapper class which contains map function
 * which produces stationID,StationTempInfo as the key,value
 */
class MeanTempMapper extends Mapper<Object,Text,Text,StationTempInfo>{
    /***
     * map : The input station record information is split by ',' to fetch individual fields. StationTempInfo object is created with flag either
     * true or false depending upon if its a max or min temperature record. The same is emitted from mapper using context.
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
            StationTempInfo station_info=new StationTempInfo(false,TMinVal);
            context.write(stationID,station_info);
        }
        else if(StationDetails.length >=4 && StationDetails[2].equals("TMAX")){
            int TMaxVal = Integer.parseInt(StationDetails[3]);
            StationTempInfo station_info=new StationTempInfo(true,TMaxVal);
            context.write(stationID,station_info);

        }
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
        // Iterate over the temperature records for the stationID key and aggregate to find average.
        for(StationTempInfo stationTempInfo:stationTempInfos){
            if(stationTempInfo.isMaxRecord){
                tMaxAvg+=stationTempInfo.temp;
                tMaxCnt+=1;
            }
            else{
                tMinAvg+=stationTempInfo.temp;
                tMinCnt+=1;
            }
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