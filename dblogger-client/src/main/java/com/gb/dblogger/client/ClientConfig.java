package com.gb.dblogger.client;

import java.util.UUID;

public class ClientConfig {
	
	public static final String client_version = "v2.0.0";
	
	private static final String PREFIX_CLIENT_ID = "default_dblogger_client";

	private long heartBeatInterval = 3000;
	
	private String addr;
	
	private String appId;
	
	private String sign;
	
	private String clientId = PREFIX_CLIENT_ID + "-" + UUID.randomUUID().toString();
	
	private String version;
	
	public void setHeartBeatInterval(long heartBeatInterval) {
		this.heartBeatInterval = heartBeatInterval;
	}
	
	public long getHeartBeatInterval() {
		return heartBeatInterval;
	}
	
	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
}
