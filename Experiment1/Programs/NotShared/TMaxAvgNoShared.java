/**
 * Created by vikasjanardhanan on 1/23/17.
 * NO-SHARING: Per-thread data structure multi-threaded version that assigns subsets of
 the input String[] (or List<String>) for processing by separate threads. Each thread
 should work on its own separate instance of the accumulation data structure. Hence no
 locks are needed. However, you need a barrier to determine when the separate threads
 have terminated and then reduce the separate data structures into a single one using
 the main thread
 */
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/***
 * Class TMaxAvgNoShared's object is created once which has TMaxStationInfo that holds each stationID to list of max temperatures
 * for the same station. 10 runs of the same operations to calculate average is performed to calculate the average run time. The load time
 * from the file is ignored.Once the list of max temperatures for each stationID is accumulated independently in each of the threads
 * , then the same information is read from those intstances and merged in main to a common data structure.
 * Then its processed to calculate the average and the result is stored in TMaxStationAvg. Only if argument is given fibonacci function
 * is run while adding the new temperature found to the stationID
 */
public class TMaxAvgNoShared {

    private static Runtime runtime = Runtime.getRuntime();
    private static final int THREAD_COUNT = runtime.availableProcessors() ;
    public static boolean use_fibonacci=false;
    private HashMap<String,List<Double>> tMaxStationInfo= new HashMap<String, List<Double>>();
    private HashMap<String,Double> tMaxStationInfoAvg= new HashMap<String, Double>();
    private static List<TemperatureWorker> tw = new ArrayList<TemperatureWorker>();

    public static void main(String[] args) throws Exception  {
        String filename="1912.csv";
        // Loads the lines in file as list of string in memory
        List<String> records = loadData(filename);
        String[] temp_info;
        List<Double> lst=new ArrayList<Double>();
        Integer avg=0;
        List<Double> temperatures = new ArrayList<Double>();

        double TotalRunTime = 0.0;
        double minRunTime=Double.MAX_VALUE;
        double maxRunTime=Double.MIN_VALUE;

        TMaxAvgNoShared TMaxAvg = new TMaxAvgNoShared();
        // Runs 10 times to calculate average RunTime
        for(int i=0;i<10;i++) {
            // Clears the hashmap entries from the previous run.
            TMaxAvg.tMaxStationInfoAvg.clear();
            TMaxAvg.tMaxStationInfo.clear();
            if (args.length > 0) {
                use_fibonacci = true;
            }
            final long startTime = System.currentTimeMillis();
            // Below code snippet splits the input list of records in the file into chunks and send each chunk to each thread which are
            // scheduled using ExecutorService
            int startIndex = 0;
            int endIndex = 0;
            int chunk_size = records.size() / THREAD_COUNT;
            endIndex = chunk_size - 1;
            int itercount = 0;
            ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
            while (itercount < THREAD_COUNT) {
                tw.add(new TemperatureWorker(records.subList(startIndex, endIndex),use_fibonacci));
                pool.execute(tw.get(itercount));
                itercount += 1;
                startIndex = endIndex;
                if (itercount == THREAD_COUNT) {
                    endIndex = records.size() - 1;
                } else {
                    endIndex += chunk_size;
                }
            }
            pool.shutdown();
            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                System.out.println("Pool interrupted!");
                System.exit(1);
            }
            TMaxAvg.mergeStationInfos();
            TMaxAvg.calculateAverage();
            final long endTime=System.currentTimeMillis();
            long execTime=endTime-startTime;
            TotalRunTime+=execTime;
            minRunTime = Math.min(minRunTime,execTime);
            maxRunTime = Math.max(maxRunTime,execTime);
            // Clear the memory for the hashmap used for each of the temperature worker threads
            for(int j=0;j<THREAD_COUNT;j++) {
                tw.get(j).tMaxStationInfo.clear();
            }
        }
        System.out.println("Average Running Time:"+TotalRunTime/10+" ms");
        System.out.println("Minimum Running Time:"+minRunTime+" ms");
        System.out.println("Maximum Running Time:"+maxRunTime+" ms");
        TMaxAvg.writeAvgValueToLog(TotalRunTime,minRunTime,maxRunTime);

    }

    /***
     * calculateAverage: void
     * Iterate over the stationInfo stored, calculate the total temperature per stationId and add the result to the tMaxStationInfoAvg
     */
    public void calculateAverage(){
        List<Double> temperatures = new ArrayList<Double>();
        Iterator it = this.tMaxStationInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            temperatures = (List<Double>) pair.getValue();
            String stationID = (String) pair.getKey();
            Double total_temp = 0.0;
            for (Double temperature : temperatures) {
                total_temp += temperature;
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
    public void writeAvgValueToLog(double totalRunTime,double minRunTime,double maxRunTime){
        String timestamp=new SimpleDateFormat("yyyy_MM_dd_hh_mm").format( new Date() );
        String filename="TMaxAvgNoShared_"+timestamp+".txt";
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
     * mergeStationInfos:
     * Iterates over each of the hashmap maintained by the threads one by one, and merges the entries into the global hashmap declared
     * at the main class.
     */
    public void mergeStationInfos(){
        HashMap<String,List<Double>> tMaxStationInfoTemp;
        List<Double> temperatures = new ArrayList<Double>();
        List<Double> temp = new ArrayList<Double>();
        for(int i=0;i<THREAD_COUNT;i++){
            tMaxStationInfoTemp = tw.get(i).tMaxStationInfo;
            Iterator it=tMaxStationInfoTemp.entrySet().iterator();

            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                temp = (List<Double>) pair.getValue();
                String stationID=(String)pair.getKey();
                if(tMaxStationInfo.containsKey(stationID)){
                    temperatures = tMaxStationInfo.get(stationID);
                    temperatures.addAll(temp);
                    tMaxStationInfo.put(stationID,temperatures);
                }
                else{
                    tMaxStationInfo.put(stationID,temp);
                }
            }

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

    /***
     * The TemperatureWorker class implements run method. It receives a chunk of station information and the common shared memory location
     * where the station information is accumulated . It iterates over these records and adds the station's max temperature information
     * whenever found into the memory location
     */
    private static class TemperatureWorker implements Runnable{
        public HashMap<String,List<Double>> tMaxStationInfo;
        private List<String> dataset;
        private Boolean use_fibonacci;

        public TemperatureWorker(List<String> dataset,Boolean use_fibonacci){
            this.tMaxStationInfo = new HashMap<String, List<Double>>();
            this.dataset = dataset;
            this.use_fibonacci=use_fibonacci;

        }

        /***
         * run: Iterates over the chunk received by this thread, for all records which are TMAX records, add the temperature for the station
         * to the list of temperatures maintained for the station. If its a new station then new list of integers is created and added
         * for the station.
         */
        @Override public void run() {
            String[] temp_info;
            String tmax_key = new String("TMAX");
            List<Double> lst = new ArrayList<Double>();
            for (String data : dataset) {
                temp_info = data.split(",");
                if (temp_info[2].equals(tmax_key) && temp_info[3] != null) {

                    if (tMaxStationInfo.containsKey(temp_info[0])) {
                        if(use_fibonacci){
                            run_fibonacci(17);
                        }
                        lst = tMaxStationInfo.get(temp_info[0]);
                        lst.add(new Double(temp_info[3]));
                        tMaxStationInfo.put(temp_info[0], lst);
                    } else {
                        List<Double> new_lst = new ArrayList<Double>();
                        new_lst.add(new Double(temp_info[3]));
                        tMaxStationInfo.put(temp_info[0],new_lst);
                    }
                }
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


    }
}
