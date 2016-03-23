package com.mytsmc.cfchange.Main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class SqlliteDB {
	
	private Connection sqliteCon;
	private static final Logger logger = Logger.getLogger(SqlliteDB.class);
	private SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH);
	private SimpleDateFormat formatDateNew = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
	
	public SqlliteDB(){
		try {
			//------------------------------------------------------------
			Class.forName("org.sqlite.JDBC");
			sqliteCon = DriverManager.getConnection("jdbc:sqlite:modifiedData.s3db");
			sqliteCon.setAutoCommit(false);
			System.out.println("-------------------- sqliteCon successful --------------------");
			logger.info("-------------------- sqliteCon successful --------------------");
			
		} catch (ClassNotFoundException ex) {
			logger.error("[SQLLite Initial] DB Initial Error : " + ex.getMessage());
		    System.err.println(ex);
		} catch (SQLException e) {	
			logger.error("[SQLLite Initial] SQL Error : " + e.getMessage());
		    e.printStackTrace();
		} 
	}
	
	public void initMemoryDB()
	{
		logger.info("Initial SQLLite DB : Create issue table");
		dropIfExist();
		initIssueTable();
	}
	
	public void close()
	{
		try {
			sqliteCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite] Close DB Error : " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private void dropIfExist()
	{
		Statement stat = null;
    	try {
        	String sql = "drop table if exists issues";
    		stat = this.sqliteCon.createStatement();
	    	stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Drop Table] Error : " + e.getMessage());			
			e.printStackTrace();
		} finally {
			try {
				if(stat != null) stat.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Drop Table] Close Cursor Error : " + e.getMessage());			
				e.printStackTrace();
			}
		}
	}
	
	// create table
    private void initIssueTable()
    {
    	Statement stat = null;
    	try {
        	String sql = "create table issues (issue_key string, subject string, updated TIMESTAMP, migrate_result string, update_issue_time string, "
        			   + "reindex_result string, update_case_center string, error_msg string)";
    		stat = this.sqliteCon.createStatement();
	    	stat.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Create Table] Table Error : " + e.getMessage());			
			e.printStackTrace();
		} finally {
			try {
				if(stat != null) stat.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Create Table] Close Cursor Error : " + e.getMessage());			
				e.printStackTrace();
			}
		}
    }

    
    public void insertRecords(String isskey, String subject, String updated)
    {
        PreparedStatement pst = null;
        try {
	        String sql = "insert into issues (issue_key, subject, updated) values (?,?,?)";
	        pst = this.sqliteCon.prepareStatement(sql);
        
	        pst.setString(1, isskey);
	        pst.setString(2, subject);
			pst.setString(3, formatDateNew.format(formatDate.parse(updated)));
			
	        pst.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Insert Record] Close Cursor Error : " + e.getMessage());			
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(pst != null) pst.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("[SQLLite Insert Record] Close Cursor Error : " + e.getMessage());			
			}
		}
    }
    
    public JSONArray getIssues(int limit)
    {
    	logger.info("[SQLLite Get Issues] Limit : " + limit);
    	JSONArray issueList = null;
    	Statement stat = null;
    	ResultSet rs = null;
    	try {
    		issueList = new JSONArray();
    		stat = sqliteCon.createStatement();
        	rs = stat.executeQuery("select issue_key, subject from issues where migrate_result is null order by updated desc LIMIT " + limit);
			while (rs.next()){
				JSONObject obj = new JSONObject();
				obj.put("key", rs.getString("issue_key"));
				obj.put("subject", rs.getString("subject"));
				issueList.put(obj);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Get Issues] Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if(stat != null) stat.close();
				if(rs != null) rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Get Issues] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
		}
    	return issueList;	
    }
    
    public void commitSqlite() {	
    	try {
			sqliteCon.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Commit] Error : " + e.getMessage());
			e.printStackTrace();
		}
    }

    public void updateResult(String issKey, int status, String resultMsg)
    {
    	String sql = "update issues set migrate_result = ?, error_msg = ? where issue_key = ?";
		logger.info("[SQLLite Update Result] Issue Key : " + issKey + ", Response Status : " + 
					status + ", Response Message : " + resultMsg);
		PreparedStatement pst = null;
    	try {
			pst = this.sqliteCon.prepareStatement(sql);
			if(status == 200) {
				pst.setString(1, resultMsg);
		        pst.setString(2, "");
		        pst.setString(3, issKey);
			}
			else {
				pst.setString(1, String.valueOf(status));
		        pst.setString(2, resultMsg);
		        pst.setString(3, issKey);				
			}
	        pst.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Update Result] Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
    		try {
				if(pst != null) pst.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Update Result] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
    	}
    	
    	commitSqlite();
    }
    
    public void updateIndexResult(String issKey, String resultMsg)
    {
    	String sql = "update issues set reindex_result = ? where issue_key = ?";
		logger.info("[SQLLite Reindex Result] Issue Key : " + issKey + ", Response Message : " + resultMsg);
    	
		PreparedStatement pst = null;
    	try {    		
			pst = this.sqliteCon.prepareStatement(sql);
			pst.setString(1, resultMsg);
			pst.setString(2, issKey);
	        pst.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Reindex Result] Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
    		try {
				if(pst != null) pst.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Reindex Result] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
    	}
    	
    	commitSqlite();
    }

    public void fixUpdatedResult(String issKey, String resultMsg, String column)
    {
    	String sql = "update issues set " + column + " = ? where issue_key = ?";
		logger.info("[SQLLite Fix Updated] Issue Key : " + issKey + ", Response Message : " + resultMsg);
    	
		PreparedStatement pst = null;
    	try {    		
			pst = this.sqliteCon.prepareStatement(sql);
			pst.setString(1, resultMsg);
			pst.setString(2, issKey);
	        pst.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("[SQLLite Fix Updated] Error : " + e.getMessage());
			e.printStackTrace();
		} finally {
    		try {
				if(pst != null) pst.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("[SQLLite Fix Updated] Close Cursor Error : " + e.getMessage());
				e.printStackTrace();
			}
    	}
    	
    	commitSqlite();
    }

}
