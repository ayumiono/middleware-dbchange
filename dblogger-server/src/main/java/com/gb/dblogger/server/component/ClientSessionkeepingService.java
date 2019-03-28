package com.gb.dblogger.server.component;

import com.gb.dblogger.remoting.ChannelEventListener;
import com.gb.dblogger.server.model.ClientSessionInfo;
import com.gb.soa.pos.dbsync.api.request.PosShopOnlioneStatusRefreshRequest;

import io.netty.channel.Channel;

public class ClientSessionkeepingService implements ChannelEventListener {
	
	private ServerController serverController;
	
	
	
	public ClientSessionkeepingService(ServerController serverController) {
		this.serverController = serverController;
	}
	
	@Override
	public void onChannelConnect(String remoteAddr, Channel channel) {
		//do nothing
	}

	@Override
	public void onChannelClose(String remoteAddr, Channel channel) {
		ClientSessionInfo sessionInfo = serverController.getClientManager().doChannelCloseEvent(remoteAddr, channel);
		if(sessionInfo == null) return;
		PosShopOnlioneStatusRefreshRequest request = new PosShopOnlioneStatusRefreshRequest();
		request.setPosShopAppKey(sessionInfo.getClientId());
		request.setStatus(1);
		this.serverController.getPosOffDbRefreshService().refreshPosShopOnlioneStatus(request);
	}

	@Override
	public void onChannelException(String remoteAddr, Channel channel) {
		ClientSessionInfo sessionInfo = serverController.getClientManager().doChannelCloseEvent(remoteAddr, channel);
		if(sessionInfo == null) return;
		PosShopOnlioneStatusRefreshRequest request = new PosShopOnlioneStatusRefreshRequest();
		request.setPosShopAppKey(sessionInfo.getClientId());
		request.setStatus(1);
		this.serverController.getPosOffDbRefreshService().refreshPosShopOnlioneStatus(request);
	}

	@Override
	public void onChannelIdle(String remoteAddr, Channel channel) {
		ClientSessionInfo sessionInfo = serverController.getClientManager().doChannelCloseEvent(remoteAddr, channel);
		if(sessionInfo == null) return;
		PosShopOnlioneStatusRefreshRequest request = new PosShopOnlioneStatusRefreshRequest();
		request.setPosShopAppKey(sessionInfo.getClientId());
		request.setStatus(1);
		this.serverController.getPosOffDbRefreshService().refreshPosShopOnlioneStatus(request);
	}
}
