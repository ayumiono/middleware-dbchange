package com.gb.dbchange.server.component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gb.dbchange.common.component.ChannelEventListener;

import io.netty.channel.Channel;

public class ClientSessionHoldService implements ChannelEventListener {
	
	private Map<String, Channel> sessions = new ConcurrentHashMap<>();

	@Override
	public void onChannelConnect(String remoteAddr, Channel channel) {
		sessions.put(remoteAddr, channel);
	}

	@Override
	public void onChannelClose(String remoteAddr, Channel channel) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onChannelException(String remoteAddr, Channel channel) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onChannelIdle(String remoteAddr, Channel channel) {
		// TODO Auto-generated method stub

	}
	
	public List<Channel> getActiveSessions(){
		return (List<Channel>) sessions.values();
	}

}
