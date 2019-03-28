package com.gb.dblogger.client.processor;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.gb.dblogger.client.DbLoggerClient;
import com.gb.dblogger.client.component.DbChangeEventListener;
import com.gb.dblogger.client.impl.DBLoggerClientApiImpl;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.remoting.exception.DBAlterTabelException;
import com.gb.dblogger.remoting.exception.DBChangeEventException;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.DBVersion;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.remoting.utils.MetricsReporter;

import io.netty.channel.ChannelHandlerContext;

public class DbChangeEventProcessor implements NettyRequestProcessor {

	private final DbChangeEventListener listener;
	
	private final DbLoggerClient client;
	
	private static final Counter successCounter = MetricsReporter.register("dbChangeEventSuccess", new Counter());
	private static final Counter totalCounter = MetricsReporter.register("dbChangeEventTotal", new Counter());
	private static final Timer dealTimer = MetricsReporter.timer("dbChangeEventHandlerTimer");
	
	public DbChangeEventProcessor(DbChangeEventListener listener,DbLoggerClient client) {
		this.listener = listener;
		this.client = client;
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		int reqCode = request.getCode();
		switch (reqCode) {
		case RequestCode.DB_LOG:
			dealDbChangeEvent(request,response);
			break;
		case RequestCode.TABLE_ALTER:
			dealTabelAlter(request,response);
			break;
		default:
			break;
		}
		return response;
	}
	
	private void dealDbChangeEvent(RemotingCommand request,RemotingCommand response) {
		DBLoggerClientApiImpl.log.debug("收到dbevent事件，并开始业务处理");
		totalCounter.inc();
		Timer.Context context = dealTimer.time();
		try {
			DbChangeEvent changeEvent = JSON.parseObject(new String(request.getBody(), Charset.forName("UTF-8")), DbChangeEvent.class);
//			Long serverSideLastDBVersion = Long.parseLong(request.getExtFields().get("version"));
//			if(client.getDbVersion() == null || client.getDbVersion() < serverSideLastDBVersion) {
//				RemotingCommand cmd = RemotingCommand.createResponseCommand(ResponseCode.DB_LOG_OVERDUE, String.format("client side last version【%s】", String.valueOf(client.getDbVersion())));
//				return cmd;
//			}
			this.listener.deal(changeEvent);
			client.setDbVersion(changeEvent.getVersion());
			DBVersion clientSideLastDBVersion = new DBVersion();
			clientSideLastDBVersion.setVersion(client.getDbVersion());
			response.writeCustomHeader(clientSideLastDBVersion);//将client版本记录回传给server端
			response.setCode(ResponseCode.DB_LOG_SUCCESS);
			successCounter.inc();
		} catch (DBChangeEventException e) {
			response.setCode(ResponseCode.DB_LOG_FAIL);
			response.setRemark(e.getErrorMessage());
		} catch (Exception e) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark(e.getMessage());
		} finally {
			DBLoggerClientApiImpl.log.debug("dbevent事件业务处理完成.");
			context.stop();
		}
	}
	
	private void dealTabelAlter(RemotingCommand request,RemotingCommand response) {
		try {
			String sql = new String(request.getBody(),Charset.forName("utf-8"));
			if(this.listener.alterTabel(sql)) {
				response.setCode(ResponseCode.DB_ALTER_TABLE_SUCCESS);
			}else {
				response.setCode(ResponseCode.DB_ALTER_TABLE_FAIL);
			}
		} catch (DBAlterTabelException e) {
			response.setCode(ResponseCode.DB_ALTER_TABLE_FAIL);
			response.setRemark(e.getErrorMessage());
		} catch (Exception e) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark(e.getMessage());
		}
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
