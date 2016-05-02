package com.tsmc.rdbdataloader;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tsmc.rdbdao.MESDBDAO;
import com.tsmc.toolfootprint.msgcreator.MsgCreator;
import com.tsmc.toolfootprint.msgformat.MsgHeaderAttrKey;
import com.tsmc.vo.AlarmVO;

import eventmq.MessageSender;

public class AlarmLoader {
	private static SimpleDateFormat sdfDateTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private static Log logger = LogFactory.getLog(AlarmLoader.class);
	
	public void loadAlarm(List<String> lstTools, int collectHistDays, String dateStart, String dateEnd){
		try {
			MessageSender mqs = new MessageSender("");
			MESDBDAO mdao = new MESDBDAO();
			for (int i = 0; i < lstTools.size(); i++){
				for (int j = 0; j < collectHistDays; j++){
					String queryStart = sdfDateTime.format(sdfDateTime.parse(dateStart).getTime() + 86400000*j);
					String queryEnd = sdfDateTime.format(sdfDateTime.parse(dateStart).getTime() + 86400000*(j+1));
					
					if (sdfDateTime.parse(queryEnd).after(sdfDateTime.parse(dateEnd))){
						queryEnd = dateEnd;
					}
					logger.info(lstTools.get(i) + " query from " + queryStart + " to " + queryEnd);
					List<AlarmVO> lstAlarmVO = mdao.queryAlarmEvents(lstTools.get(i), queryStart, queryEnd);
					sendMESFail(lstTools.get(i), lstAlarmVO, mqs);
					logger.info(lstTools.get(i) + " " + lstAlarmVO.size() + " ALM records");
				}
			}
		} catch (Exception ex){
			ex.printStackTrace();
			logger.error(ex.getMessage());
		}
	}

	private void sendMESFail(String toolId, List<AlarmVO> lstAlarmVO, MessageSender mqs) throws Exception{
		//System.out.print(lstAlarmVO.size());
		for (int i = 0; i < lstAlarmVO.size(); i++){
			MsgCreator mc = new MsgCreator("PROCESS", "MESDB", lstAlarmVO.get(i).AlarmType, toolId, "ALMLoader");
			mc.setHeaderAttr(MsgHeaderAttrKey.units, lstAlarmVO.get(i).Chamber);
			mc.setHeaderAttr(MsgHeaderAttrKey.eventcreatetime, sdfDateTime.format(lstAlarmVO.get(i).AlmTime));
			mc.setHeaderAttr(MsgHeaderAttrKey.duration, "0");
			mc.setHeaderAttr(MsgHeaderAttrKey.eventputtime, sdfDateTime.format(new Date(System.currentTimeMillis())));
			mc.setHeaderAttr(MsgHeaderAttrKey.eventdescription, lstAlarmVO.get(i).AlmDesc);
			//System.out.println(mc.getXmlContent());
			mqs.SendMessage(mc.getXmlContent());
		}
	}

}
