package com.mytsmc.cfchange.Main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONException;

public class Main {
	
	private static final Logger logger = Logger.getLogger(Main.class);
	static DBOperation dbOp;
	static JIRAOperation jiraOp;
	static SqlliteDB sqlDbOp;
	

	public static void main(String[] args) throws IOException {
		File log4jfile = new File("log4j.properties");
		PropertyConfigurator.configure(log4jfile.getAbsolutePath());
		SimpleDateFormat formatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
		
		// Initialization
		dbOp = new DBOperation();
		sqlDbOp = new SqlliteDB();
		jiraOp = new JIRAOperation();
		
		Date start = new Date();
		
		// load issue keys
		System.out.println("Get Issue Information from JIRA......");
		logger.info("Get Issue Information from JIRA......");
		sqlDbOp.initMemoryDB();		
		readIssueList();
		sqlDbOp.commitSqlite();
		
		Date end = new Date();
		long timediff = end.getTime() - start.getTime();
		System.out.println("Time spent: " + (timediff / 1000 + (timediff % 1000) * 0.001) + " seconds.");
		logger.info("[Query Issues] Time spent : " + (timediff / 1000 + (timediff % 1000) * 0.001) + " seconds.");

		
		try {
			System.out.println("Start edit issue summary......");
			JSONArray issueList = sqlDbOp.getIssues(1000);
			while(issueList.length() > 0) {
				for(int i=0; i<issueList.length(); i++) {
					String key = issueList.getJSONObject(i).getString("key");
					String subject = issueList.getJSONObject(i).getString("subject");
					String subject_new = subject + " ---";
					Timestamp updated = dbOp.getIssueUpdated(key);
//					System.out.println(updated);
					
					// Step 1 : Update subject value add ---
					jiraOp.editIssueSummary(key, subject_new, sqlDbOp);
					Thread.sleep(1000);
					
					// Step 2 : Update subject value to origin
					jiraOp.editIssueSummary(key, subject, sqlDbOp);
										
					// Step 3 : Recover Update Time
					System.out.println("Recover update time ......");
					String result = dbOp.updateIssueTime(updated, key);
					jiraOp.doReindex(key, sqlDbOp);
					sqlDbOp.fixUpdatedResult(key, result, "update_issue_time");
					
					Thread.sleep(5000);

					// Step 4 : Recover Case Center Update Time
					System.out.println("Recover case center update time ......");
					result = dbOp.updateCaseCenter(updated, key);
					sqlDbOp.fixUpdatedResult(key, result, "update_case_center");

					
				}
	
				end = new Date();
				System.out.println("Current Time : " + formatTime.format(end));
				logger.info("Current Time : " + formatTime.format(end));	
				
//				Thread.sleep(300000);
//				System.out.println("Sleep 5 minutes ......... ");	
				
				issueList = sqlDbOp.getIssues(1000);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.error("[Main] JSON Parse Error : " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("[Thread Error] " + e.getMessage());
			e.printStackTrace();
		}
		
		end = new Date();
		timediff = end.getTime() - start.getTime();
		System.out.println("Time spent: " + (timediff / 1000 + (timediff % 1000) * 0.001) + " seconds.");
		logger.info("Time spent : " + (timediff / 1000 + (timediff % 1000) * 0.001) + " seconds.");

		
		sqlDbOp.close();
		dbOp.close();
	}
	
	public static void readIssueList()
	{
		BufferedReader br = null;		
		try {
			br = new BufferedReader(new FileReader("issueKey.txt"));
		    String line = br.readLine();

		    while (line != null) {
		    	jiraOp.getIssueSummary(line, sqlDbOp);		    	
		        line = br.readLine();
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    try {
				if(br != null) br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
