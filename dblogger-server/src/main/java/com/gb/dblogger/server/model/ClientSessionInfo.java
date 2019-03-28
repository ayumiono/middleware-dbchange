package com.gb.dblogger.server.model;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.gb.dblogger.remoting.protocol.RemotingSerializable;

import io.netty.channel.Channel;

public final class ClientSessionInfo extends RemotingSerializable {
	
	private final String clientId;
	private final String clientVersion;
	private final Channel channel;
	private final String remoteAddress;
	private final Map<String, Object> attachments;
	
	private Set<String> subscribes = new ConcurrentHashSet<>();
	
	public boolean isIntrested(String eventType) {
		return subscribes.contains(eventType);
	}
	
	public void subscribe(String... eventType) {
		subscribes.addAll(Arrays.asList(eventType));
	}
	
	public void subscribe(Set<String> eventType) {
		subscribes.addAll(eventType);
	}

	public static final String DBLOG_VERSION = "dblog_version";
	
	public static final String JETTY_CONTAINER_STATUS = "container_status";
	

	public ClientSessionInfo(String clientId, String clientVersion, Channel channel, String remoteAddress) {
		this.clientId = clientId;
		this.clientVersion = clientVersion;
		this.channel = channel;
		this.remoteAddress = remoteAddress;
		this.attachments = new ConcurrentHashMap<>();
	}

	public String getClientId() {
		return clientId;
	}
	
	public String getClientVersion() {
		return clientVersion;
	}

	public Channel getChannel() {
		return channel;
	}

	public String getRemoteAddress() {
		return remoteAddress;
	}

	public Map<String, Object> getAttachments() {
		return attachments;
	}

	public void putAttachment(String key, Object param) {
		this.attachments.put(key, param);
	}

	public Object getAttachment(String key) {
		return this.attachments.get(key);
	}

	public void dblogversion(long version) {
		Long lastVersion = this.dblogversion();
		if (lastVersion == null || lastVersion <= version) {
			this.putAttachment(DBLOG_VERSION, version);
		}
	}

	public long dblogversion() {
		if(this.getAttachment(DBLOG_VERSION) == null) return 0;
		return (Long) this.getAttachment(DBLOG_VERSION);
	}
	
	public void jettycontainerStatus(boolean status) {
		this.putAttachment(JETTY_CONTAINER_STATUS, status);
	}
	
	public boolean jettycontainerStatus() {
		if(this.getAttachment(JETTY_CONTAINER_STATUS) == null) return false;
		return (Boolean) this.getAttachment(JETTY_CONTAINER_STATUS);
	}
}
