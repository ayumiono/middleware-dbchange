package com.gb.dblogger.client;

import com.gb.dblogger.client.processor.EventListener;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.remoting.RemotingClient;
import com.gb.dblogger.remoting.netty.NettyClientConfig;
import com.gb.dblogger.remoting.netty.NettyRemotingClient;
import com.gb.dblogger.remoting.protocol.HeartBeatData;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RemotingSerializable;
import com.gb.dblogger.remoting.protocol.RequestCode;

public class POSDbSyncClient extends AbstractCommonClient {

	public POSDbSyncClient(RemotingClient client) {
		super(client);
	}

	@Override
	protected HeartBeatData generateHeartBeatData() {
		return new HeartBeatData();
	}
	
	public static void main(String[] args) {
		POSDbSyncClient client = new POSDbSyncClient(new NettyRemotingClient(new NettyClientConfig()));
		client.setClientId("");
		client.setAddr("");
		client.setSign("");
		client.setHeartBeatInterval(3000);
		
		client.subscribe(RequestCode.DB_LOG_EVENT,RequestCode.TABLE_ALTER_EVENT);
		EventListener listener = new EventListener() {
			
			@Override
			protected Object decode(RemotingCommand request) throws Exception {
				if(request.getCode() == RequestCode.DB_LOG) {
					DbChangeEvent event = RemotingSerializable.decode(request.getBody(), DbChangeEvent.class);
					return event;
				}else if(request.getCode() == RequestCode.TABLE_ALTER) {
					String sql = RemotingSerializable.decode(request.getBody(), String.class);
					return sql;
				}
				throw new Exception("解码失败");
			}
			
			@Override
			protected boolean deal(Object t, RemotingCommand request, RemotingCommand response) throws Exception {
				if(t instanceof String ) {
					//TODO
				}else if(t instanceof DbChangeEvent) {
					//TODO
				}
				return false;
			}
		};
		client.registerEventListener(RequestCode.DB_LOG, listener);
		client.registerEventListener(RequestCode.TABLE_ALTER, listener);
		client.start();
	}

}
