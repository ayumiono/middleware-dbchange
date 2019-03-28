package com.gb.dblogger.client.impl;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.client.AbstractCommonClient;
import com.gb.dblogger.client.DbLoggerClient;
import com.gb.dblogger.remoting.exception.HeartBeatException;
import com.gb.dblogger.remoting.exception.RemotingException;
import com.gb.dblogger.remoting.protocol.HeartBeatData;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.protocol.ResponseCode;

/**
 * db logger client api
 * 
 * @author xuelong.chen
 *
 */
public class CommonClientApiImpl {
	
	public static final String DBLOGGER_CLIENT_API = "DbLoggerClientApi";

	public static final Logger log = LoggerFactory.getLogger(DBLOGGER_CLIENT_API);

	private final AbstractCommonClient client;

	public CommonClientApiImpl(AbstractCommonClient client) {
		this.client = client;
	}

	public int heartBeat(HeartBeatData heartBeat) throws RemotingException, InterruptedException, HeartBeatException {
		log.debug("开始发送心跳包");
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
		request.setBody(heartBeat.encode());
		attachAuthInfoTo(request);
		RemotingCommand response = client.getRemotingClient().invokeSync(this.client.getAddr(), request, 10000);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS: {
				return response.getVersion();
			}
			default:
				break;
		}
		throw new HeartBeatException(response.getCode(), response.getRemark());
	}

	public boolean subscribe(String...eventTypes) throws Exception {
		log.debug("开始订阅");
		try {
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SUBSCRIBE_EVENT, null);
			request.setBody(StringUtils.join(eventTypes, ",").getBytes("utf-8"));
			attachAuthInfoTo(request);
			RemotingCommand response = client.getRemotingClient().invokeSync(this.client.getAddr(), request, 10000);
			assert response != null;
			switch (response.getCode()) {
				case ResponseCode.SUCCESS: {
					return true;
				}
				default:
					break;
			}
		} catch (Exception e) {
			log.error("订阅失败", e);
			throw e;
		}
		return false;
	}
	
	public void attachAuthInfoTo(RemotingCommand request) {
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", client.getAppId());
		extfield.put("sign", client.getSign());
		extfield.put("version",DbLoggerClient.client_version);
		request.setExtFields(extfield);
	}
}
