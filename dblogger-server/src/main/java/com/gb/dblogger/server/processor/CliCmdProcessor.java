package com.gb.dblogger.server.processor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.gb.dblogger.common.protocol.LibUpdEvent;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.DBLoggerServerStatus;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RemotingSerializable;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.remoting.utils.TokenUtil;
import com.gb.dblogger.server.component.ServerController;
import com.gb.dblogger.server.dubbo.DubboInboundApiProvider;

import io.netty.channel.ChannelHandlerContext;

public class CliCmdProcessor implements NettyRequestProcessor {
	
	ServerController controller;
	
	public CliCmdProcessor(ServerController controller) {
		this.controller = controller;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
		switch (request.getCode()) {
		case RequestCode.DBLOGGER_SERVER_STATUS:
			snapshot(request,response);
			break;
		case RequestCode.GEN_AUTH:
			authtoken(request,response);
			break;
		case RequestCode.LIB_UPD_STATUS:
			libupdstatus(request,response);
			break;
		case RequestCode.CLEAR_LIB_UPD_CACHE:
			clearLibUpdCache(request, response);
			break;
		case RequestCode.LIB_UPD_CACHE:
			libUpdCache(request,response);
			break;
		case RequestCode.JETTY_CONTAINER_STATUS:
			jettyContainerStatus(request,response);
		default:
			break;
		}
		return response;
	}
	
	private void jettyContainerStatus(RemotingCommand request,final RemotingCommand response) {
		DBLoggerServerStatus status = controller.jettyContainerStatus();
		response.setBody(status.encode());
		response.setRemark("jetty container status");
	}
	
	/**
	 * 查询所有客户端的lib upd状态
	 * @param request
	 * @param response
	 */
	private void libupdstatus(RemotingCommand request,final RemotingCommand response) {
		//TODO
	}
	
	private void clearLibUpdCache(RemotingCommand request,final RemotingCommand response) {
		try {
			String lib2clear = new String(request.getBody(), "utf-8");
			if(lib2clear.equals("*")) {
				DubboInboundApiProvider.clearLibUpdCache();
				response.setRemark("清空缓存成功.");
			}else {
				DubboInboundApiProvider.removeLibUpdCache(lib2clear);
				response.setRemark("删除缓存成功.");
			}
		} catch (Exception e) {
			response.setCode(ResponseCode.TRANSACTION_FAILED);
			response.setRemark("删除缓存失败"+e.getMessage());
		}
	}
	
	private void libUpdCache(RemotingCommand request,final RemotingCommand response) {
		List<LibUpdEvent> tmp = new ArrayList<>();
		DubboInboundApiProvider.latestLibs().forEach(new Consumer<LibUpdEvent>() {
			@Override
			public void accept(LibUpdEvent arg0) {
				tmp.add(arg0);
			}
		});
		response.setBody(RemotingSerializable.encode(tmp));
	}
	
	private void snapshot(RemotingCommand request,final RemotingCommand response) {
		DBLoggerServerStatus status = controller.snapshot();
		response.setBody(status.encode());
		response.setRemark("server snapshot status");
	}
	
	private void authtoken(RemotingCommand request,final RemotingCommand response) {
		int countneed = request.getExtFields().get("count") == null ? 0 : Integer.parseInt(request.getExtFields().get("count"));
		if(countneed > 100 || countneed == 0) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("you can only request 0-100 tokens one time");
			return;
		}
		List<String[]> tokens = new ArrayList<>();
		for(int i=0;i<countneed;i++) {
			String appId = TokenUtil.generateAppid();
			String sign = TokenUtil.generateAppkey(appId);
			tokens.add(new String[] {appId,sign});
		}
		response.setBody(JSON.toJSONString(tokens, false).getBytes(Charset.forName("UTF-8")));
		response.setRemark("auth tokens");
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}
}
