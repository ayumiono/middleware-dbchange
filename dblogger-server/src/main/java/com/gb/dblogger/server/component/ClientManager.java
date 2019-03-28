package com.gb.dblogger.server.component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Counter;
import com.gb.dblogger.common.protocol.LibUpdEvent;
import com.gb.dblogger.remoting.protocol.HeartBeatData;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.utils.MetricsReporter;
import com.gb.dblogger.server.dubbo.DubboInboundApiProvider;
import com.gb.dblogger.server.model.ClientSessionInfo;
import com.gb.soa.pos.dbsync.api.request.PosShopOnlioneStatusRefreshRequest;

import io.netty.channel.Channel;

/**
 * 
 * 管理客户端连接session
 * @author xuelong.chen
 *
 */
public class ClientManager {
	
	private ConcurrentMap<String, ClientSessionInfo> sessions = new ConcurrentHashMap<>();
	private static final Counter sessionCounter = MetricsReporter.register("sessions", new Counter());
	
	private ServerController controller;
	
	public ClientManager(ServerController controller) {
		this.controller = controller;
	}
	
	public Iterator<ClientSessionInfo> observers(String eventType){
		List<ClientSessionInfo> result = new ArrayList<>();
		for(ClientSessionInfo session : sessions.values()) {
			if(session.isIntrested(eventType)) {
				result.add(session);
			}
		}
		return result.iterator();
	}
	
	public ClientSessionInfo doChannelCloseEvent(final String remoteAddr,final Channel channel) {
		sessionCounter.dec();
		return sessions.remove(remoteAddr);
	}
	
	public ClientSessionInfo getSession(String remoteAddress) {
		return sessions.get(remoteAddress);
	}
	
	/**
	 * 根据不同的订阅，调用不同的心跳包处理逻辑
	 * @param heartBeatData
	 * @param channel
	 * @param remoteAddress
	 */
	public void doHeartBeat(HeartBeatData heartBeatData, Channel channel, String remoteAddress) {
		ClientSessionInfo session = sessions.get(remoteAddress);
		if(session == null) return;
		if(session.isIntrested(RequestCode.LIB_UPD_EVENT)) {
			boolean containerStatus = (Boolean) heartBeatData.getAttachment("container_status");
			session.jettycontainerStatus(containerStatus);
		}
	}
	
	public void doSubscription(String client,String clientVersion, String subscribe,Channel channel, String remoteAddress) {
		ClientSessionInfo session = new ClientSessionInfo(client,clientVersion,channel,remoteAddress);
		session.subscribe(subscribe.trim().split(","));
		if(!sessions.containsKey(remoteAddress)) {
			sessionCounter.inc();
			sessions.put(remoteAddress, session);
			if(session.isIntrested(RequestCode.LIB_UPD_EVENT)) {//补推一次最新的lib更新给新订阅的或重新订阅的客户端
				List<LibUpdEvent> updEvents = new ArrayList<>(DubboInboundApiProvider.latestLibs());
				if(updEvents.size() > 0) {
					this.controller.notifyLibUpd(updEvents, session, null);
				}
			}
		}
		PosShopOnlioneStatusRefreshRequest request = new PosShopOnlioneStatusRefreshRequest();
		request.setPosShopAppKey(client);
		request.setStatus(0);
		this.controller.getPosOffDbRefreshService().refreshPosShopOnlioneStatus(request);
	}
	
	public Collection<ClientSessionInfo> activeSessions(){
		return sessions.values();
	}
}
