package com.gb.dblogger.remoting.protocol;

import java.util.Map;

public class DBLoggerServerStatus extends RemotingSerializable {
	private int sessions;
	private Map<String, Object> versions;
	public int getSessions() {
		return sessions;
	}
	public void setSessions(int sessions) {
		this.sessions = sessions;
	}
	public Map<String, Object> getVersions() {
		return versions;
	}
	public void setVersions(Map<String, Object> versions) {
		this.versions = versions;
	}
}
