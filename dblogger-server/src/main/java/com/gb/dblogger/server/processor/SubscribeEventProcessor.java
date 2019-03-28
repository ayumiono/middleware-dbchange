package com.gb.dblogger.server.processor;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.remoting.common.RemotingHelper;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.server.component.ServerController;

import io.netty.channel.ChannelHandlerContext;

public class SubscribeEventProcessor implements NettyRequestProcessor {
	
	private static final Logger log = LoggerFactory.getLogger(SubscribeEventProcessor.class);
	
	private ServerController controller;
	
	public SubscribeEventProcessor(ServerController controller) {
		this.controller = controller;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		log.info("处理订阅");
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		Map<String, String> extFields = request.getExtFields();
		String client = extFields.get("appId");
		String version = extFields.get("version");
		String subscribe = new String(request.getBody(),"utf-8");
//		SubscribeEvent subscribe = SubscribeEvent.decode(request.getBody(), SubscribeEvent.class);
		String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
//		controller.getClientManager().doSubscribeEvent(subscribe, ctx.channel(), remoteAddress);
		controller.getClientManager().doSubscription(client,version,subscribe,ctx.channel(), remoteAddress);
		response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
