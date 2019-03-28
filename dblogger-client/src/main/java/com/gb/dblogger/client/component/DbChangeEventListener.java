package com.gb.dblogger.client.component;

import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.remoting.exception.DBAlterTabelException;
import com.gb.dblogger.remoting.exception.DBChangeEventException;

public interface DbChangeEventListener {
	long deal(DbChangeEvent dbLog) throws DBChangeEventException;
	boolean alterTabel(String sql) throws DBAlterTabelException;
}
