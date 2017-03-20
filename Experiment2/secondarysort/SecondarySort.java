
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

/***
 * SecondarySort : Driver class for mapreduce program
 *
 */
public class SecondarySort
{

    /***
     * Main : Setups up the mapreduce environment configuration.
     * This program uses secondarysort approach. The Map output key is (stationID,year), output value is of type StationTempInfo
     * which has year,min max aggregate and its count.Input file names and output directory configuration is read from args
     * @param args : contains two parameters input file name and the output directory name.
     */
    public static void main( String[] args )
    {

        try {
            Configuration conf = new Configuration();
            BasicConfigurator.configure();
            GenericOptionsParser optionsParser = new GenericOptionsParser(conf,args);
            String[] remainingArgs = optionsParser.getRemainingArgs();
            if (remainingArgs.length < 2){
                System.err.println("Usage: hadoop jar secondarysort.jar <in> [<in>...] <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "Mean Temperature");
            job.setJarByClass(SecondarySort.class);
            job.setMapperClass(SecSortMapper.class);
            job.setCombinerClass(SecSortCombiner.class);
            job.setGroupingComparatorClass(SecSortGroupComparator.class);
            job.setPartitionerClass(SecSortPartitioner.class);
            job.setReducerClass(SecSortReducer.class);
            job.setOutputKeyClass(StationYearKey.class);
            job.setOutputValueClass(StationTempInfo.class);
            for(int i=0;i<remainingArgs.length-1;i++) {
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length-1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

/***
 * StationTempInfo: Value type class.
 * Contains aggregate maxtemp,mintemp, the counts for each of those and the year.
 */
class StationTempInfo implements Writable{
    public int maxtemp;
    public int mintemp;
    public long maxCnt;
    public long minCnt;
    String year;

    public StationTempInfo()
    {

    }
    public StationTempInfo(int maxtemp,int mintemp,long maxCnt,long minCnt,String year){
        this.maxtemp=maxtemp;
        this.mintemp=mintemp;
        this.maxCnt=maxCnt;
        this.minCnt=minCnt;
        this.year=year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(maxtemp);
        dataOutput.writeInt(mintemp);
        dataOutput.writeLong(maxCnt);
        dataOutput.writeLong(minCnt);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        maxtemp=dataInput.readInt();
        mintemp=dataInput.readInt();
        maxCnt=dataInput.readLong();
        minCnt=dataInput.readLong();
        year=dataInput.readUTF();
    }
}

/***
 * StationYearKey : Key type Class which has the fields stationID and year for which the temperature
 * is recorded.
 */
class StationYearKey implements Writable,WritableComparable{
    public String stationID;
    public String year;

    public StationYearKey()
    {

    }
    public StationYearKey(String stationID,String year){
        this.stationID=stationID;
        this.year=year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeUTF(stationID);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        stationID=dataInput.readUTF();
        year=dataInput.readUTF();
    }
    @Override
    public int compareTo(Object other) {
        return this.compareTo((StationYearKey) other);
    }

    /***
     * compareTo : Compares the incoming key with current object. Compares station first.If same
     * then compares with year and returns the result.
     * @param stationYearKey
     * @return
     */
    public int compareTo(StationYearKey stationYearKey){
        int cmp = this.stationID.compareTo(stationYearKey.stationID);
        if(cmp==0){
            return this.year.compareTo(stationYearKey.year);
        }
        return cmp;
    }
}


/***
 * SecSortMapper: Mapper Class. Emits (stationID,year) key and value StationTempInfo having temperature information
 * for the stationID and year.
 */
class SecSortMapper extends Mapper<Object,Text,StationYearKey,StationTempInfo>{
    /***
     * map: The input record containing station information is split by ',' to fetch individual fields. StationTempInfo object is created
     * to which the current record's maxtemp or mintemp is added with other two fields set as 0
     * This is emitted as value with key being (stationID,year)
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        String[] StationDetails;
        StationDetails=value.toString().split(",");
        Text stationID=new Text();
        stationID.set(StationDetails[0]);
        String year=StationDetails[1].substring(0,4);
        StationYearKey stationYearKey=new StationYearKey(StationDetails[0],year);
        if(StationDetails.length >=4 && StationDetails[2].equals("TMIN")){
            int TMinVal = Integer.parseInt(StationDetails[3]);
            StationTempInfo station_info=new StationTempInfo(0,TMinVal,0,1,year);
            context.write(stationYearKey,station_info);
        }
        else if(StationDetails.length >=4 && StationDetails[2].equals("TMAX")){
            int TMaxVal = Integer.parseInt(StationDetails[3]);
            StationTempInfo station_info=new StationTempInfo(TMaxVal,0,1,0,year);
            context.write(stationYearKey,station_info);

        }
    }
}

/***
 * Partition class
 */
class SecSortPartitioner extends Partitioner<StationYearKey,StationTempInfo>{
    /***
     * getPartition: partitions to record to specific reducer based on the stationID
     * @param stationYearKey : (stationID,year)
     * @param stationTempInfo: Temperature information held
     * @param numPartitions : No of reducers
     * @return
     */
    @Override
    public int getPartition(StationYearKey stationYearKey,StationTempInfo stationTempInfo,int numPartitions){
        return (stationYearKey.stationID.hashCode() & Integer.MAX_VALUE)%numPartitions;
    }
}

/***
 * Combiner class
 */
class SecSortCombiner extends Reducer<StationYearKey,StationTempInfo,StationYearKey,StationTempInfo> {
    /***
     * reduce: List of objects for given key - (stationID,year) is combined into single object and emitted
     * @param key : (stationID,year)
     * @param stationTempInfos - List of temperature information for given key.
     * @param context : used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(StationYearKey key, Iterable<StationTempInfo> stationTempInfos, Context context) throws IOException,InterruptedException{
        StationTempInfo combined_stationInfo = new StationTempInfo();
        for(StationTempInfo stationTempInfo:stationTempInfos){
            combined_stationInfo.maxtemp+=stationTempInfo.maxtemp;
            combined_stationInfo.mintemp+=stationTempInfo.mintemp;
            combined_stationInfo.maxCnt+=stationTempInfo.maxCnt;
            combined_stationInfo.minCnt+=stationTempInfo.minCnt;
            combined_stationInfo.year=stationTempInfo.year;
        }
        context.write(key,combined_stationInfo);
    }
}

/***
 * KeyComparator Class
 */
class SecSortKeyComparator extends WritableComparator{
    protected SecSortKeyComparator()
    {
        super(StationYearKey.class,true);
    }

    /***
     * compare : Sorts by stationID first then by year
     * @param w1 : Key object 1 to be compared
     * @param w2 : Key object 2 to be compared
     * @return
     */
    public int compare(WritableComparable w1,WritableComparable w2){
        StationYearKey k1=(StationYearKey) w1;
        StationYearKey k2 = (StationYearKey) w2;
        return k1.compareTo(k2);
    }
}

/***
 * GroupingComparator Class
 */
class SecSortGroupComparator extends WritableComparator{
    protected SecSortGroupComparator()
    {
        super(StationYearKey.class,true);
    }

    /***
     * compare : Groups key by stationID, so that all outputs with same stationID gets send in the same
     * reduce call irrespective of the year.
     * @param w1 : Key 1 to compare
     * @param w2 : Key 2 to compare
     * @return
     */
    @Override
    public int compare(WritableComparable w1,WritableComparable w2){
        StationYearKey k1=(StationYearKey) w1;
        StationYearKey k2=(StationYearKey) w2;
        return k1.stationID.compareTo(k2.stationID);
    }
}

/***
 * Reducer Class
 */
class SecSortReducer extends Reducer<StationYearKey,StationTempInfo,NullWritable,Text> {
    /***
     * reduce : Iterates over the list of values for given stationID, since records are in sorted order of
     * year. Keeps accumulating till year changes. Then creates result string as the year changes to produce the result
     * string for the given stationID
     * @param key : (stationID,year) key
     * @param stationTempInfos : List of temperature information for given stationID
     * @param context: used to emit output
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(StationYearKey key, Iterable<StationTempInfo> stationTempInfos, Context context) throws IOException,InterruptedException{

        String cur_year=null;
        double tMaxAvg=0.0;
        double tMinAvg=0.0;
        long tMaxCnt=0;
        long tMinCnt=0;
        String result=key.stationID+", [";
        for(StationTempInfo stationTempInfo:stationTempInfos){
            if(cur_year!=null && !cur_year.equals(key.year)){
                result+="("+cur_year+", ";
                if(tMinCnt>0){
                    tMinAvg=tMinAvg/tMinCnt;
                    result+=tMinAvg+", ";
                }
                else{
                    result+="0, ";
                }
                if(tMaxCnt>0){
                    tMaxAvg=tMaxAvg/tMaxCnt;
                    result+=tMaxAvg;
                }
                else{
                    result+="0";
                }
                result+=")";
                tMaxAvg=0;
                tMinAvg=0;
                tMaxCnt=0;
                tMinCnt=0;
                result+=", ";
            }
            tMaxAvg+=stationTempInfo.maxtemp;
            tMinAvg+=stationTempInfo.mintemp;
            tMaxCnt+=stationTempInfo.maxCnt;
            tMinCnt+=stationTempInfo.minCnt;
            cur_year=stationTempInfo.year;
        }

        result+="("+cur_year+", ";
        if(tMinCnt>0){
            tMinAvg=tMinAvg/tMinCnt;
            result+=tMinAvg+", ";
        }
        else{
            result+="null, ";
        }
        if(tMaxCnt>0){
            tMaxAvg=tMaxAvg/tMaxCnt;
            result+=tMaxAvg;
        }
        else{
            result+="null";
        }

        Text output = new Text(result+")]");
        context.write(NullWritable.get(),output);
    }
}
