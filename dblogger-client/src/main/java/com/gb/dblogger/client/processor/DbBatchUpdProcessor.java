package com.gb.dblogger.client.processor;

import com.gb.dblogger.client.DbLoggerClient;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.DBVersion;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.ResponseCode;

import io.netty.channel.ChannelHandlerContext;

public class DbBatchUpdProcessor implements NettyRequestProcessor {
	
	private final DbLoggerClient client;
	
	public DbBatchUpdProcessor(DbLoggerClient client) {
		this.client = client;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		RemotingCommand cmd = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
		client.setDbVersion(50L);//FIXME
		DBVersion version = new DBVersion();
		version.setVersion(50L);
		cmd.writeCustomHeader(version);
		cmd.setOpaque(request.getOpaque());
		return cmd;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
