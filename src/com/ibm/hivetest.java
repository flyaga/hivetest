package com.ibm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class hivetest {
	static Logger logger = LoggerFactory.getLogger(hivetest.class);
	
	public void run() throws Exception {
		String driverName = //"org.apache.hive.jdbc.HiveDriver";
				"oracle.jdbc.OracleDriver";
		try {
			Class.forName(driverName.trim());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = null;
		try {
			con = DriverManager.getConnection(
					//"jdbc:oracle:thin:@192.168.0.241:1521:MAORCL","newma","password");
					"jdbc:oracle:thin:@127.0.0.1:1521:MAORCL","newma","password");
					//"jdbc:hive2://192.168.0.242:10000/default", "root", "ssss");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Statement stmt = con.createStatement();
		
		//stmt.executeQuery("use psbcma1");
		
		String sql="drop table if exists BIZF_CC2LOB_HB_MC_HL";
		//stmt.execute(sql);
		sql="create table BIZF_CC2LOB_HB_MC_HL (AS_OF_DATE string,RULE_CODE string,DRIVER_CODE string, ORG_L1 String, ORG_ID String,ORG_L2 String,ORG_L3 String, LOB_ID String, SRC_ID String, COST_AMT double ) stored as parquet";
		//stmt.execute(sql);
		sql="insert into table BIZF_CC2LOB_HB_MC_HL select adate,rule_code,driver_code, ORG_L1, ORG_ID,ORG_L2,ORG_L3, LOB_ID, '123', sum(cost_amt) from( select substr(a.as_of_date,1,10) adate,'HB_MC_HL' rule_code,a.driver_code, a.ORG_L1, a.ORG_ID,a.ORG_L2,a.ORG_L3, b.LOB_ID, '123', round(c.DRIVER_RATIO*b.COST_AMT*a.DRIVER_AMT/d.sum_driver_amt,2) as cost_amt from bizf_driver a,BIZF_COST_CENTER b,DEF_RULE_DRIVER c,  ( select driver_code,ORG_L1,sum(DRIVER_AMT) as sum_driver_amt from BIZF_DRIVER group by driver_code,ORG_L1 ) d where c.rule_code='HB_MC_HL' and a.driver_code=c.driver_code and a.driver_code=d.driver_code and a.ORG_L1=d.ORG_L1 and b.ORG_L1=d.ORG_L1 ) e group by e.adate,e.rule_code,e.driver_code, e.ORG_L1, e.ORG_ID,e.ORG_L2,e.ORG_L3, e.LOB_ID order by adate,rule_code,driver_code, ORG_L1, ORG_ID,ORG_L2,ORG_L3, LOB_ID";
		sql="select * from DEF_DIM_MEMBER";
		//stmt.execute(sql);
		sql="select count(*) from  DEF_DIM_MEMBER";  
		//sql="select * from smg_def_rule";
		ResultSet res = stmt.executeQuery(sql);  		
	    while (res.next()) {  
	      System.out.println(res.getString(1));  
	    } 
	}
	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main1(String[] args) throws Exception {
		hivetest ahivetest=new hivetest();
		ahivetest.run();
	}
	
	public static void main(String[] args) throws Exception {
		//System.out.println(StringUtil.decodeBase64("MQ=="));
		
		if(1==0)
			return;
		String flowProgram="C:\\edisk\\test\\test.bat";
		if(args.length>0)
			flowProgram=args[0];
		String errorMessage="";
		Process process=null;
		try{
			process = Runtime.getRuntime().exec(flowProgram);
			
			InputStream stdErr = process.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stdErr);
			BufferedReader br = new BufferedReader(isr);
			String line=null;
			while ((line = br.readLine()) != null)
				errorMessage=errorMessage+line+"\n";
			if(process.waitFor()!=0){
				if(errorMessage.equals("")){
					errorMessage="error run";
				}
			}
		}catch(Exception e){
			errorMessage=e.toString();//.getMessage();
		}
		//System.out.println(errorMessage);
		if(errorMessage.trim().length()>0)
			logger.error(errorMessage);
	}
}
