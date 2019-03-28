package com.gb.dblogger.remoting.protocol;

import java.util.HashMap;
import java.util.Map;

public class HeartBeatData extends RemotingSerializable {
	
	private Map<String, Object> attachments;
	
	public void addAttachment(String key, Object value) {
		if(attachments == null) {
			this.attachments = new HashMap<>();
		}
		this.attachments.put(key, value);
	}
	
	public Object getAttachment(String key) {
		if(this.attachments == null) return null;
		return this.attachments.get(key);
	}

	public Map<String, Object> getAttachments() {
		return attachments;
	}

	public void setAttachments(Map<String, Object> attachments) {
		this.attachments = attachments;
	}
}
