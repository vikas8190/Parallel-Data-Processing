/**
 * Created by vikasjanardhanan on 1/23/17.
 *
SEQ: Sequential version that calculates the average of the TMAX temperatures by
        station Id. Pick a data structure that makes sense for grouping the accumulated
        temperatures and count of records by station. We will refer to this as the accumulation
        data structure.
 */

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

/***
 * Class TMaxAvgSequential's object is created once which has tMaxStationInfoMap that holds each stationID to list of max temperatures
 * for the same station. 10 runs of the same operations to calculate average is performed to calculate the average run time. The load time
 * from the file is ignored.Once the list of max temperatures for each stationID is accumulated, then its processed to calculate the average
 * and the result is stored in tMaxStationInfoAvg. Only if argument is given fibonacci function is run while adding the new temperature found
 * to the stationID
 */
public class TMaxAvgSequential {
    private HashMap<String,List<Double>> tMaxStationInfoMap= new HashMap<String, List<Double>>();
    private HashMap<String,Double> tMaxStationInfoAvg= new HashMap<String, Double>();

    public static boolean use_fibonacci = false;
    public static void main(String[] args) throws Exception  {
        String filename="1912.csv";
        // Loads the lines in file as list of string in memory
        List<String> records = loadData(filename);
        String[] StationInfo;
        Double temperature;
        String StationID;

        double TotalRunTime = 0.0;
        double minRunTime=Double.MAX_VALUE;
        double maxRunTime=Double.MIN_VALUE;
        TMaxAvgSequential TMaxAvg = new TMaxAvgSequential();

        // Runs 10 times to calculate average RunTime
        for(int i=0;i<10;i++) {
            // Clears the hashmap entries from the previous run.
            TMaxAvg.tMaxStationInfoAvg.clear();
            TMaxAvg.tMaxStationInfoMap.clear();
            if (args.length > 0) {
                use_fibonacci = true;
            }
            final long startTime=System.currentTimeMillis();

            // For each of the line loaded, split by , to seperate out parts and add to StationInfo map.
            for (String data : records) {
                StationInfo = data.split(",");
                if (StationInfo.length >= 4 && StationInfo[2].equals(new String("TMAX"))) {
                    temperature = Double.parseDouble(StationInfo[3]);
                    StationID = StationInfo[0];
                    TMaxAvg.addToTmaxStationInfo(StationID, temperature);
                }
            }
            TMaxAvg.calculateAverage();
            final long endTime=System.currentTimeMillis();
            long execTime=endTime-startTime;
            TotalRunTime+=execTime;
            minRunTime = Math.min(minRunTime,execTime);
            maxRunTime = Math.max(maxRunTime,execTime);
        }
        System.out.println("Average Running Time:"+TotalRunTime/10+" ms");
        System.out.println("Minimum Running Time:"+minRunTime+" ms");
        System.out.println("Maximum Running Time:"+maxRunTime+" ms");
        TMaxAvg.writeAvgValuesToLog(TotalRunTime,minRunTime,maxRunTime);
    }

    /***
     * calculateAverage: void
     * Iterate over the stationInfo stored, calculate the total temperature per stationId and add the result to the tMaxStationInfoAvg
     */
    public void calculateAverage(){

        Iterator it = this.tMaxStationInfoMap.entrySet().iterator();
        List<Double> temperatures = new ArrayList<Double>();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            temperatures = (List<Double>) pair.getValue();
            String stationID = (String) pair.getKey();
            Double total_temp = 0.0;
            for (Double temp : temperatures) {
                total_temp += temp;
            }
            this.tMaxStationInfoAvg.put(stationID, total_temp / temperatures.size());
            it.remove();
        }
    }

    /***
     * writeAvgValueToLog : Double Double Double
     * Checks if logs/ directory exists, creates if doesnt exist. Then iterates over the average temperatures per StationID and adds
     * the result to file in logs directory. The filename suffix is maintainted as the timestamp when the execution happens.
     * The Average, min and max runtimes are also written to the file.
     * @param totalRunTime: Double -> Total Run time of the program among the 10 runs.
     * @param minRunTime: Double -> minimum Run time of the program among the 10 runs.
     * @param maxRunTime: Double -> Maximum Run time of the program among the 10 runs.
     */
    public void writeAvgValuesToLog(double totalRunTime,double minRunTime,double maxRunTime){
        String timestamp=new SimpleDateFormat("yyyy_MM_dd_hh_mm").format( new Date() );
        String filename="TMaxAvgSequential_"+timestamp+".txt";
        File directory = new File("logs");
        if(!directory.exists()){
            directory.mkdir();
        }
        File log_file=new File("logs/"+filename);
        String stationID;
        Double avgTemp;
        try{
            FileWriter fw = new FileWriter(log_file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            Iterator it = this.tMaxStationInfoAvg.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                stationID=pair.getKey().toString();
                avgTemp=Double.parseDouble(pair.getValue().toString());
                bw.write(stationID+"|"+avgTemp+"\n");
                it.remove();
            }
            bw.write("Average Running Time:"+totalRunTime/10+" ms\n");
            bw.write("Minimum Running Time:"+minRunTime+" ms\n");
            bw.write("Maximum Running Time:"+maxRunTime+" ms\n");
            bw.close();
        }
        catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }


    }

    /***
     * The given stationID information is added to the hashmap. If the stationID doesnt exist new entry is created in the hashmap
     * else the current list of entries for the stationID is updated to contain the new temperature currently given as parameter
     * @param StationID: String -> Current StationID read
     * @param temperature: Double -> Given StationID's max temperature reading.
     */
    public void addToTmaxStationInfo(String StationID,Double temperature){
        if(use_fibonacci){
            run_fibonacci(17);
        }
        List<Double> lst=new ArrayList<Double>();
        if(tMaxStationInfoMap.containsKey(StationID)){
            lst = tMaxStationInfoMap.get(StationID);
            lst.add(new Double(temperature));
            tMaxStationInfoMap.put(StationID,lst);
        }
        else{
            List<Double> new_lst=new ArrayList<Double>();
            new_lst.add(new Double(temperature));
            tMaxStationInfoMap.put(StationID,new_lst);
        }
    }

    /***
     * run_fibonacci : Executes fibonacci number generation upto the given n fibonacci numbers
     * @param n : The number of fibonacci number sequences to be calculated
     * @return : nth number in fibonacci series.
     */
    public static int run_fibonacci(int n){
        if(n==0){
            return 0;
        }
        else if(n==1){
            return 1;
        }
        else{
            return run_fibonacci(n-1)+run_fibonacci(n-2);
        }
    }

    /***
     * loadData(String): Reads the supplied filename line by line and appends the result to array list and returns the same at the end
     * @param filename: String -> The filename which contains the station information which is to be read.
     * @return: List<String> -> which contains all the lines in the file which was read.
     * @throws Exception
     */
    public static List<String> loadData(String filename) throws Exception{
        List<String> records;
        records = new ArrayList<String>();
        BufferedReader br = null;

        try{
            br=new BufferedReader(new FileReader(filename));
            StringBuilder sb =  new StringBuilder();
            String line = br.readLine();
            while(line!=null){
                records.add(line);
                line=br.readLine();
            }

        }   finally {
            br.close();
        }
        return records;
    }
}
