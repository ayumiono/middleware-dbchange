package com.gb.dblogger.remoting.protocol;

import com.gb.dblogger.remoting.CommandCustomHeader;
import com.gb.dblogger.remoting.exception.RemotingCommandException;

public class DBVersion implements CommandCustomHeader {
	
	private Long version;

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public void checkFields() throws RemotingCommandException {
		
	}

}
