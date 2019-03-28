package com.gb.dblogger.remoting.protocol;

public class RequestCode {
	public static final int HEART_BEAT = 100;
	
	public static final int SUBSCRIBE_EVENT = 101;

	public static final String DB_LOG_EVENT = "DB_LOG";
	
	public static final int DB_LOG = 200;

	public static final String TABLE_ALTER_EVENT = "TABLE_ALTER";
	
	public static final int TABLE_ALTER = 201;
	
	public static final String LIB_UPD_EVENT = "LIB_UPD";
	
	public static final int LIB_UPD = 202;

	public static final int DB_OVERDUE = 300;

	/**
	 * 生成鉴权
	 */
	public static final int GEN_AUTH = 400;

	/**
	 * 获取所有客户端数据同步版本信息
	 */
	public static final int DBLOGGER_SERVER_STATUS = 500;

	/**
	 * 获取所有客户端lib版本信息
	 */
	public static final int LIB_UPD_STATUS = 501;
	
	/**
	 * 清空服务器中lib upd缓存数据
	 */
	public static final int CLEAR_LIB_UPD_CACHE = 502;
	
	/**
	 * 查看服务器中lib upd缓存数据
	 */
	public static final int LIB_UPD_CACHE = 503;
	
	public static final int JETTY_CONTAINER_STATUS = 504;
}
