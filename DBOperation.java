package com.mytsmc.cfchange.Main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBOperation {
	private Connection connection;
	private static final Logger logger = Logger.getLogger(DBOperation.class);
	
	public DBOperation() {
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("config.properties");

			// load a properties file
			prop.load(input);
			
			String dbConnection = prop.getProperty("database");
			String dbUser = prop.getProperty("dbuser");
			String dbPassword = prop.getProperty("dbpassword");
			
			// get the property value and print it out
			System.out.println("-------------------- Database Information --------------------");
			System.out.println("Connection : " + dbConnection);
			System.out.println("Account    : " + dbUser);

			
			logger.info("-------------------- Database Information --------------------");
			logger.info("Connection : " + dbConnection);
			logger.info("Account    : " + dbUser);			
			
			try {
				connection = DriverManager.getConnection(dbConnection, dbUser, dbPassword);
				System.out.println("Connect to database successfully.");
				logger.info("Connect to database successfully.");
//				writer.write("Connect to database successfully.\n");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("Open Connection Error : " + e.getMessage());
				e.printStackTrace();
			}
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

	
	public void close()
	{
		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Close DB Connection Error : " + e.getMessage());
			e.printStackTrace();
		}
	}	
	
	public Timestamp getIssueUpdated(String key)
	{
		logger.info("[Get Issue Update Time] Issue Key : " + key);
		Statement stmt = null;
		ResultSet rs = null;
		Timestamp updated = null;
		try {
			stmt = connection.createStatement();
			String sql = "select updated from jiraissue where pkey = '" + key + "'";
			rs = stmt.executeQuery(sql);
			while(rs.next())
				updated = rs.getTimestamp("updated");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[Get Issue Update Time] SQL Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) rs.close();
				if(stmt != null) stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[Get Issue Update Time] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		if(updated == null) 
			logger.error("[Get Issue Update Time] Issue " + key + " not found.");
		else {
			String updateTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(updated);
			logger.info("[Get Issue Update Time] Update Time : " + updateTime);
		}
		
		return updated;
	}
	
	public String updateIssueTime(Timestamp updated, String key)
	{
		logger.info("[Update Issue Updated] Issue Key : " + key);
		PreparedStatement stmt = null;
		String result = "Fail";
		try {
			String sql = "UPDATE jiraissue SET UPDATED=? WHERE pkey = '" + key + "'";
			stmt = connection.prepareStatement(sql);
			stmt.setTimestamp(1, updated);
			stmt.executeUpdate();
			result = "Success";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[Update Issue Updated] SQL Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[Update Issue Updated] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
		}
		return result;
	}	
	
	public String updateCaseCenter(Timestamp updated, String key)
	{
		logger.info("[Update Case Center] Issue Key : " + key);
		PreparedStatement stmt = null;
		String result = "Fail";
		try {
			String sql = "UPDATE tckm_cc_jira_issue SET UPDATE_DT=? WHERE jira_issue_key = '" + key + "'";
			stmt = connection.prepareStatement(sql);
			stmt.setTimestamp(1, updated);
			stmt.executeUpdate();
			result = "Success";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[Update Case Center] SQL Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[Update Case Center] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
		}
		return result;
	}

}
