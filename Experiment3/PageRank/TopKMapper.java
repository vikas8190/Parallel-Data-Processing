/**
 * Created by vikasjanardhanan on 2/20/17.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;


import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/***
 * TopKMapper stores local top 100 pages based on pagerank value. Uses priority queue for the same.
 */
public class TopKMapper extends Mapper<Text,PageNode,NullWritable,PageRankNode> {
    private PriorityQueue<PageRankNode> topPages;

    /***
     * setup :  Initializes the priority queue with custom comparator
     * @param context
     */
    public void setup(Context context){
        Comparator<PageRankNode> comparator= new PageNodeComparator();
        this.topPages= new PriorityQueue<PageRankNode>(100,comparator);
    }

    /***
     * map: Adds each incoming pagename to the priority queue and removes from the queue is the size exceeds 100.
     * @param key
     * @param pageNode
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Text key, PageNode pageNode, Context context) throws IOException, InterruptedException {
        topPages.add(new PageRankNode(key.toString(),pageNode.pageRank));
        if(topPages.size()>100){
            topPages.poll();
        }
    }

    /***
     *  cleanup: Emits the local top 100 from the mapper
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void cleanup(Context context) throws IOException,InterruptedException{
        PageRankNode page;
        while(!topPages.isEmpty()){
            page=topPages.poll();
            context.write(NullWritable.get(),page);
        }

    }

    /***
     * Custom comparator class which results in records being stored in increasing order of pagerank value in the
     * priority queue.
     */
    public static class PageNodeComparator implements Comparator<PageRankNode>{

        public int compare(PageRankNode x,PageRankNode y){
            if(x.pageRank < y.pageRank){
                return -1;
            }
            if(x.pageRank > y.pageRank){
                return 1;
            }
            return 0;
        }
    }
}
