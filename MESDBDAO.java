package com.tsmc.rdbdao;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tsmc.vo.AlarmVO;
import com.tsmc.vo.EMSFailVO;
import com.tsmc.vo.EQPMainStateVO;
import com.tsmc.vo.EQPSubStateVO;
import com.tsmc.vo.OCAPTriggerVO;

public class MESDBDAO {
	private static Log logger = LogFactory.getLog(MESDBDAO.class);
	
	public List<AlarmVO> queryAlarmEvents(String eqpId, String dateStart, String dateEnd) throws Exception {
		List<AlarmVO> lstAlarmVO = new ArrayList<AlarmVO>();
		
		try {
			Properties pr = new Properties();
			pr.load(new FileInputStream("config/sql.txt"));
			String QueryAlarm = pr.getProperty("QueryAlarm");
		
			
			DBConnector dbcn = new DBConnector();
			Connection cn = dbcn.ConnMESDB();;			
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			String sql = QueryAlarm;
			pstmt = cn.prepareStatement(sql);
			pstmt.setString(1, eqpId+"%");
			pstmt.setString(2, dateStart);
			pstmt.setString(3, dateEnd);
			rs = pstmt.executeQuery();
			while (rs.next()){		
				AlarmVO obj = new AlarmVO();
				obj.EqpId = rs.getString("TOOL");
				obj.Chamber = rs.getString("CHAMBER");
				obj.AlarmType = rs.getString("ALMSOURCETYPE")+"_ALARM";
				obj.AlmTime = rs.getTimestamp("ALMTM");
				obj.AlmId = rs.getString("ALMID");
				obj.AlmDesc = rs.getString("ALMDESC");
				lstAlarmVO.add(obj);
			}
			rs.close();
			pstmt.close();
		} catch (Exception ex){
			ex.printStackTrace();
			logger.error(ex.getMessage());
			throw ex;
		}
		return lstAlarmVO;
	}

}
