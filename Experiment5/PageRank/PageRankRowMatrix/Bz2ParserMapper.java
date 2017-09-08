import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
/**
 * Created by vikasjanardhanan on 2/20/17.
 */

/***
 * Bz2ParserMapper is responsible for reading the page,source_code details from the file and converting it to pair of
 * (pagename,PageNode) where PageNode contains details about its adjacency list and whether its a sink node or not
 */
public class Bz2ParserMapper extends Mapper<Object,Text,Text,PageNodeValue> {

    // Expected regex for a pagename which is valid
    private static Pattern namePattern;
    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
    }

    /***
     * map : Splits the input line to pagename and html combination. If the pagename doesnt contain "~" then only it
     * proceeds. It then uses SAXParser to parse the html and extract the outgoing links from the current page. Emits
     * record for each outgoing link with value PageNode where isSink is set as true and adjacency list is empty.
     * Also emits record for the current pagename with PageNode where adjacency list has all the pages discovered on
     * parsing.
     * @param key :Object type key
     * @param value: contains the pagename,source_Code information from Bz2 file
     * @param context: used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        String line=value.toString();
        int delimLoc = line.indexOf(':');
        String pageName = line.substring(0, delimLoc);
        String html = line.substring(delimLoc + 1);
        html = html.replaceAll(" & ","&amp;");
        Matcher matcher = namePattern.matcher(pageName);
        if (!matcher.find()) {
            // Skip this html file, name contains (~).
            return;
        }

        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            ArrayList<String> linkPageNames = new ArrayList<String>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            // Parse page and fill list of linked pages.
            linkPageNames.clear();
            xmlReader.parse(new InputSource(new StringReader(html)));
            PageNodeValue node=new PageNodeValue();
            node.isSink=false;
            node.adjList=linkPageNames;
            context.write(new Text(pageName),node);
            // Creates PageNode with isSink set and emits for each of the outgoing link.
            for(String linkedpage:linkPageNames){
                PageNodeValue linkedNode=new PageNodeValue();
                linkedNode.adjList=new ArrayList<String>();
                linkedNode.isSink=true;
                context.write(new Text(linkedpage),linkedNode);
            }
        }
        catch (Exception e) {
            // Discard ill-formatted pages.
            System.out.println("invalid :" + html);
        }
    }
}
