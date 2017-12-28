import java.io.* ;
import java.util.*;
import java.time.*;
import java.lang.Character;

import javax.xml.stream.XMLInputFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.extras.codecs.*;
import com.datastax.driver.mapping.*;
import com.xml.parser.*;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;




public class WebSearch{

	public static class CompositeWritable implements Writable {
	    public String url = "";
	    public int relevance = 0;

	    public CompositeWritable() {}

	    public CompositeWritable(int relevance, String url) {
	        this.url = url;
	        this.relevance = relevance;
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        relevance = in.readInt();
	        url = WritableUtils.readString(in);
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeInt(relevance);
	        WritableUtils.writeString(out, url);
	    }

	    @Override
	    public String toString() {
	        return this.relevance + "\t" + this.url + "\t";
	    }
	}


	public static class WebSearchMapper extends Mapper<Object, Text, Text, CompositeWritable>{

		private static List<String> STOPWORDS = 
							new ArrayList<String>(Arrays.asList("Wikipedia", "the", "and", "for"));

		public static class CassandraClient{
			Session session = null;
			PreparedStatement insert_page_statement = null;

			public CassandraClient(){}

			public void connect(){
				Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				Metadata metadata = cluster.getMetadata();
				session = cluster.connect();
				prepareStatement();
			}

			public void close(){
				session.getCluster().close();
				session.close();
			}

			public void prepareStatement(){
				String query = "INSERT INTO wikipedia.pages " +
							   "(url, title, abstract, length, refs) " +
							   "VALUES (?, ?, ?, ?, ?);";

				insert_page_statement = session.prepare(query);
			}

			public void insertPage(String url, String title, String text_abstract, int length, int refs){
				session.execute(insert_page_statement.bind(url, title, text_abstract, length, refs));
			}
		}

		static CassandraClient cassandra_client = new CassandraClient();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			cassandra_client.connect();
			
			try {
 
	            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
	            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	            Document doc = dBuilder.parse(is);
	 
	            doc.getDocumentElement().normalize();
	            NodeList nList = doc.getElementsByTagName("doc");
	 
	            for (int temp = 0; temp < nList.getLength(); temp++) {
	                Node nNode = nList.item(temp);
	                
	                String title, url, text_abstract;
	                title = url = text_abstract = "";
	                
	                int refs, length, relevance;
	                refs = length = relevance = 0;
	                
	                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	                    Element eElement = (Element) nNode;
	                    title = eElement.getElementsByTagName("title").item(0).getTextContent();
	                    url = eElement.getElementsByTagName("url").item(0).getTextContent();
	                    text_abstract = eElement.getElementsByTagName("abstract").item(0).getTextContent();	 
	                    refs = eElement.getElementsByTagName("links").item(0).getChildNodes().getLength();		
	 					length = text_abstract.length();
	                }
	               	relevance = length * refs;
 					cassandra_client.insertPage(url, title, text_abstract, length, refs);

 					String[] txt = (title + text_abstract).toLowerCase().split(" ");
 					List<String> words = new ArrayList<String>();

 					for (String w : txt){
 						w = w.trim().replaceAll("[^a-zA-Z]","");
 						if (!STOPWORDS.contains(w) && w.length() > 2){
 							words.add(w);
 						}
 					}

 					for (String word : words){
 						context.write(new Text(word), new CompositeWritable(relevance, url));
 					}
	            }
        	}
	        catch (Exception e) {
	            System.out.println(e.getMessage());
	        }

			cassandra_client.close();
		}
	}

	public static class WebSearchReducer extends Reducer<Text,CompositeWritable,Text, CompositeWritable> {

		public static class CassandraClient{
			Session session = null;
			PreparedStatement insert_page_statement = null;

			public CassandraClient(){}

			public void connect(){
				Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				Metadata metadata = cluster.getMetadata();
				session = cluster.connect();
				prepareStatement();
			}

			public void close(){
				session.getCluster().close();
				session.close();
			}

			public void prepareStatement(){
				String query = "INSERT INTO wikipedia.inverted " +
							   "(keyword, relevance, url) " +
							   "VALUES (?, ?, ?);";

				insert_page_statement = session.prepare(query);
			}

			public void insertPage(String keyword, int relevance, String url){
				session.execute(insert_page_statement.bind(keyword, relevance, url));
			}
		}

		static CassandraClient cassandra_client2 = new CassandraClient();

		public void reduce(Text key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
			cassandra_client2.connect();

			Map<Integer, String> map = new TreeMap<Integer,String>();

			for (CompositeWritable c : values){
				map.put(c.relevance, c.url);
			}

			int i = 0;
			for (Map.Entry<Integer, String> entry : map.entrySet()){
				if (i > 10) break;
				cassandra_client2.insertPage(key.toString(), entry.getKey(), entry.getValue());
				context.write(key, new CompositeWritable(entry.getKey(), entry.getValue()));
				i++;
			}
			cassandra_client2.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("START_TAG_KEY", "<doc>");
        conf.set("END_TAG_KEY", "</doc>");

		Job job = new Job(conf, "search");
		job.setJarByClass(WebSearch.class);
		job.setMapperClass(WebSearchMapper.class);
		job.setReducerClass(WebSearchReducer.class);
		job.setInputFormatClass(XmlInputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(CompositeWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(CompositeWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}