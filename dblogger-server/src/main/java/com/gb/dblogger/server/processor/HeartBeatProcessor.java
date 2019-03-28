package com.gb.dblogger.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.remoting.common.RemotingHelper;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.HeartBeatData;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.server.component.ServerController;

import io.netty.channel.ChannelHandlerContext;

public class HeartBeatProcessor implements NettyRequestProcessor {
	
	public static final String HEARTBEAT_PROCESSOR = "HeartBeatProcessor";
	
	public static final Logger log = LoggerFactory.getLogger(HEARTBEAT_PROCESSOR);
	
	private ServerController controller;
	
	public HeartBeatProcessor(ServerController controller) {
		this.controller = controller;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		HeartBeatData heartBeatData = HeartBeatData.decode(request.getBody(), HeartBeatData.class);
		String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
		controller.getClientManager().doHeartBeat(heartBeatData, ctx.channel(), remoteAddress);
		response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
