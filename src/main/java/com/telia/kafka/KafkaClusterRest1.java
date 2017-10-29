package com.telia.kafka;


import java.io.FileNotFoundException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Vector;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


@Path("/kafkabrokers")
public class KafkaClusterRest1 {

	private Logger log = Logger.getLogger(KafkaClusterRest1.class);
	private final String CONNECTION_URL = "connection_url";
	private final String ZK_ID = "zk_id";
	private final String LOADBALANCER_HOSTNAME= "loadbalancer_hostname";
	private final String ACTUAL_HOSTNAME= "actual_hostname";
	private final String SEARCH_START_KEY = "search_start_key";
	private final String SEARCH_END_KEY = "search_end_key";
	private final String ERROR_PAGE = "error_page";
	private java.net.URI location= null ;
	
    @GET
    @Produces(MediaType.TEXT_HTML)
    public String getBrokers() throws IOException, KeeperException, 
                                  InterruptedException, URISyntaxException  {
    	String outStr = null;
    	
    	String html = null;
    	
    	try  {
    		
    	   	ZooKeeper zk = new ZooKeeper(PropertyHandler.getInstance().getValue(CONNECTION_URL),1000, null);
	
    	   	String brokerInfo = "";
    	   	
    	   	Vector brokerVector = new Vector();
	
    	   	List<String> ids = zk.getChildren(PropertyHandler.getInstance().getValue(ZK_ID), true);
	
    	   	for (String id : ids)  {
		
    	   		brokerInfo = new String(zk.getData(PropertyHandler.getInstance().getValue(ZK_ID)
	    			                 .concat("/") + id, false, null));
    	   		brokerVector.add(brokerInfo);
    	   	}	

    	   	getBrokerNodes(brokerVector);
    	   
    	   	outStr = getBrokerNodes(brokerVector).toString().replace('[', ' ');
	
    	   	outStr = outStr.replace(']',  ' ');
    	   	
    	   	System.out.println("Bootstrap URL : " + outStr);
    	
    	}
    	
    	catch (Exception exception)  {
    		
    		log.warn("*****Exception Occured*******");
    		
    		location = new java.net.URI(PropertyHandler.getInstance().getValue(ERROR_PAGE));
    		
    		throw new WebApplicationException(Response.temporaryRedirect(location).build());
    		
    	}
	    		
	    return outStr;
    }
	
    private Vector getBrokerNodes(Vector brokerVector) throws FileNotFoundException,IOException, URISyntaxException  {

    	Vector lbVector = null;
    	String hostString = null;
    	
    	try {
    	
    		lbVector = new Vector();
    	
    		for (int index = 0; index < brokerVector.size(); index++)  {

    			String tokenString = (String) brokerVector.get(index);
    	
    			int beginIndex = tokenString.lastIndexOf(PropertyHandler.getInstance().getValue(SEARCH_START_KEY));
      		
    			int endIndex = tokenString.indexOf(PropertyHandler.getInstance().getValue(SEARCH_END_KEY));
    		
    			hostString = tokenString.substring(beginIndex, endIndex);
    		
    			hostString = hostString.replaceFirst(PropertyHandler.getInstance().getValue(SEARCH_START_KEY), "");
    		
    			hostString = hostString.replace(PropertyHandler.getInstance().getValue(ACTUAL_HOSTNAME), 
                    PropertyHandler.getInstance().getValue(LOADBALANCER_HOSTNAME));
	
    			lbVector.add(hostString); 
    		}

    	}catch(Exception e) {
    	
    		log.warn("*****Exception Occured*******");
    	
    		location = new java.net.URI(PropertyHandler.getInstance().getValue(ERROR_PAGE));
    	
    		throw new WebApplicationException(Response.temporaryRedirect(location).build());
		
    	}
     
        	return lbVector;
    }
  
}
