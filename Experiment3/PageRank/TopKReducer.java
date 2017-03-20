/**
 * Created by vikasjanardhanan on 2/24/17.
 */
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.ArrayList;

/***
 * TopKReducer combines local top 100 pages into global top 100 and produces the final result
 */
public class TopKReducer extends Reducer<NullWritable,PageRankNode,NullWritable,Text>{
    private Comparator<PageRankNode> comparator= new TopKMapper.PageNodeComparator();
    private PriorityQueue<PageRankNode> topPages= new PriorityQueue<PageRankNode>(100,comparator);

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
        PageRankNode pageNode;

        List<PageRankNode> result=new ArrayList<PageRankNode>();
        while(!topPages.isEmpty()) {
            pageNode = topPages.poll();
            result.add(pageNode);
        }
        for(int i=result.size()-1;i>=0;i--){
            pageNode=result.get(i);
            //context.write(NullWritable.get(),new Text(pageNode.pageName+":"+pageNode.pageRank));
            context.write(NullWritable.get(),new Text(String.format("%s:%.15f",pageNode.pageName,pageNode.pageRank)));
        }


    }


}
