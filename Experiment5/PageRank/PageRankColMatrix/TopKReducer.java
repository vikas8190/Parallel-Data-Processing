/**
 * Created by vikasjanardhanan on 2/24/17.
 */
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;


/***
 * TopKReducer combines local top 100 pages into global top 100 and produces the final result
 */
public class TopKReducer extends Reducer<NullWritable,PageRankNode,NullWritable,Text>{
    private Comparator<PageRankNode> comparator= new TopKMapper.PageNodeComparator();
    private PriorityQueue<PageRankNode> topPages;
    private MapWritable numToPageNameMap;


    public void setup(Context context) throws IOException{
        this.topPages= new PriorityQueue<PageRankNode>(100,comparator);
        this.numToPageNameMap = new MapWritable();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        URI[] paths = context.getCacheFiles();
        FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
        this.formNumToPageMapers(files, conf);
    }


    /***
     * reduce: Adds each PageNode to the priority queue. Removes from the queue if size is > 100
     * Emits the global top 100 at the end.
     * @param key: pagename
     * @param pageNodes: PageNode corresponding to the pagename
     * @param context: used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(NullWritable key,Iterable<PageRankNode> pageNodes,Context context) throws IOException,InterruptedException{
        for(PageRankNode pageNode:pageNodes){
            //topPages.add(pageNode);
            topPages.add(new PageRankNode(pageNode.pageName,pageNode.pageRank));
            if(topPages.size()>100){
                topPages.poll();
            }
        }


    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        PageRankNode pageNode;

        List<PageRankNode> result=new ArrayList<PageRankNode>();
        while(!topPages.isEmpty()) {
            pageNode = topPages.poll();
            result.add(pageNode);
        }
        for(int i=result.size()-1;i>=0;i--){
            pageNode=result.get(i);
            //context.write(NullWritable.get(),new Text(pageNode.pageName+":"+pageNode.pageRank));
            //context.write(NullWritable.get(),new Text(String.format("%s:%.15f",pageNode.pageName,pageNode.pageRank)));
            context.write(NullWritable.get(), new Text(Double.toString(pageNode.pageRank) +":" + this.numToPageNameMap.get(new LongWritable(pageNode.pageName)).toString()));
        }
    }

    public void formNumToPageMapers(FileStatus[] files, Configuration conf) throws IOException {
        for (int i = 0; i < files.length; i++) {
            Path curFile = files[i].getPath();
            if (curFile.toString()
                    .contains(PageRankColMatrix.PageNameToNumberOutputDir + PageRankColMatrix.NumberToStringDir)) {
                SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(curFile));
                System.out.println("status: " + i + " " + curFile.toString());
                NullWritable key = NullWritable.get();
                MapWritable value = new MapWritable();
                double danglingMass = 0.0;
                while (seqFileReader.next(key, value)) {
                    this.numToPageNameMap.putAll(value);
                    System.out.println("put :"+value);
                }
                seqFileReader.close();
            }
        }
    }


}
