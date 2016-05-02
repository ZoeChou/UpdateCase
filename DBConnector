package com.tsmc.rdbdao;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import com.tsmc.hbase.pump.util.SecureUtil;

public class DBConnector {
	private static Connection connMESDB = null;
	
	public static void CloseAllConnections() throws Exception{
		if (connMESDB != null){connMESDB.close();}
	}

	public Connection ConnMESDB() throws Exception {
		if (connMESDB == null || connMESDB.isClosed()){
			Properties pr = new Properties();
			pr.load(new FileInputStream("config/db.properties"));
			String driver = "oracle.jdbc.driver.OracleDriver"; //Oracle
			Class.forName(driver).newInstance();
			String cnStr = pr.getProperty("MESDB").trim();
			connMESDB = DriverManager.getConnection(cnStr, pr.getProperty("MESDB_Account").trim(), 
												    SecureUtil.decode(pr.getProperty("MESDB_Password").trim()));
		}
		return connMESDB;
	}
}

