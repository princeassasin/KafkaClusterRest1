package com.telia.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertyHandler{

    private static PropertyHandler propertyHandler = null;
    
    private Properties props = null;
    
	private PropertyHandler() throws FileNotFoundException, 
	                                 IOException  {
		
		this.props = new Properties();
		
		props.load(KafkaClusterRest1.class.getResourceAsStream("/property/kafka.properties"));
	}

	public static synchronized PropertyHandler getInstance() throws FileNotFoundException, IOException  {
		
	    if (propertyHandler == null)  {
	    	
	        propertyHandler = new PropertyHandler();
	    }
	    return propertyHandler;
	}

	public String getValue(String propKey) {
		
	    return this.props.getProperty(propKey);
    }
}