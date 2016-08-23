package com.mytsmc.cfchange.Main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


public class JIRAOperation {
	private String baseURL;
	private String user;
	private String password;
	private static final Logger logger = Logger.getLogger(JIRAOperation.class);

	
	public JIRAOperation() {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream("config.properties");

			// load a properties file
			prop.load(input);			
			baseURL = prop.getProperty("JIRA_URL");
			if(baseURL.endsWith("/")) baseURL = baseURL.substring(0, baseURL.length()-1);
			user = prop.getProperty("JIRA_Account");
			password = prop.getProperty("JIRA_Password");
			
			// get the property value and print it out
			System.out.println("-------------------- JIRA Information --------------------");
			System.out.println("URL : " + baseURL);
			System.out.println("Account    : " + user);
			
			logger.info("-------------------- JIRA Information --------------------");
			logger.info("URL : " + baseURL);
			logger.info("Account    : " + user);
			


		} catch (IOException ex) {
			logger.error("Read Config File Error : " + ex.getMessage());
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error("Close Config File Error : " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}	
	
	public void editIssueSummary(String issueKey, String subject, SqlliteDB sqlDbOp) 
	{
		logger.info("[Edit Issue] Issue Key : " + issueKey + ", Summary Value : " + subject);
		try {
	        String tsUrl = baseURL + "/rest/tsmc/2.0/utility/doEdit/" + issueKey;
	        
	        /**
	         * {    
			 *	    "Summary": "[SCDHold] T9H812.00 PESTL5 REPM07  -----  test",    
			 *	    "customfield": {}
			 *	}    
	         */
	        
	        String input = "{\"Summary\": \"" + subject + "\",\"customfield\": {}}";
			logger.info("[Edit Issue] Service Input : " + input);   
	        
	        String auth = "Basic " + new String(Base64.encodeBase64(new String(user + ":" + password).getBytes("UTF-8")));

            Client client = Client.create();
            WebResource webResource = client.resource(tsUrl);      
            ClientResponse response = webResource.type("application/json").header("Authorization", auth).post(ClientResponse.class, input);
            String output = response.getEntity(String.class);
            int status = response.getStatus();
            
            logger.info("[Edit Issue] Response Status : " + status + ", Response Message : " + output);
            System.out.println("Issue " + issueKey + ", Response Status : " + status + ", Response Message : " + output);
			
            sqlDbOp.updateResult(issueKey,status, output);
            
		} catch (Exception e) {
			logger.error("[Edit Issue] Error : " + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void getIssueSummary(String issueKey, SqlliteDB sqlDbOp)
	{
		logger.info("[Get Issue Summary] Issue Key : " + issueKey);
		try {
	        String tsUrl = baseURL + "/rest/api/2/issue/" + issueKey + "?fields=summary,updated";
	        String auth = "Basic " + new String(Base64.encodeBase64(new String(user + ":" + password).getBytes("UTF-8")));

            Client client = Client.create();
            WebResource webResource = client.resource(tsUrl);      
            ClientResponse response = webResource.type("application/json").header("Authorization", auth).get(ClientResponse.class);
            String output = response.getEntity(String.class);
            int status = response.getStatus();
            
            logger.info("[Get Issue Summary] Response Status : " + status);
            System.out.println("Issue " + issueKey + ", result status : " + status);
            
            JSONObject issueInfo = new JSONObject(output);
            String subject = issueInfo.getJSONObject("fields").getString("summary");
            String updated = issueInfo.getJSONObject("fields").getString("updated");
            
            sqlDbOp.insertRecords(issueKey, subject, updated);
            
            System.out.println("Subject = " + subject + ", Updated = " + updated);            
            logger.info("[" + issueKey + "] Subject = " + subject + ", Updated = " + updated);


		} catch (Exception e) {
			logger.error("[Migrate Field Value] Error : " + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void doReindex(String issueKey, SqlliteDB sqlCon)
	{
		logger.info("[Rebuild Index] Issue Key : " + issueKey);
		try {
			String tsUrl = baseURL + "/rest/tsmc/2.0/utility/doReindex/" + issueKey;
	        String auth = "Basic " + new String(Base64.encodeBase64(new String(user + ":" + password).getBytes("UTF-8")));
	        
            Client client = Client.create();
            WebResource webResource = client.resource(tsUrl);      
            ClientResponse response = webResource.type("application/json").header("Authorization", auth).get(ClientResponse.class);
            String output = response.getEntity(String.class);
            int status = response.getStatus();
            
            System.out.println("Issue " + issueKey + ", reindex result : " + status + ", result message : " + output);
    		logger.info("[Rebuild Index] Response Status : " + status + ", Response Message : " + output);
			
            sqlCon.updateIndexResult(issueKey, output);
		}
		catch(Exception e) {
			logger.error("[Rebuild Index] Error : " + e.getMessage());
			e.printStackTrace();
		}
	}

}
