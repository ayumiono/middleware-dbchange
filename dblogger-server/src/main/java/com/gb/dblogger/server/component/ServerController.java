package com.gb.dblogger.server.component;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.fastjson.JSON;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.common.protocol.LibUpdEvent;
import com.gb.dblogger.remoting.ChannelEventListener;
import com.gb.dblogger.remoting.InvokeCallback;
import com.gb.dblogger.remoting.exception.RemotingSendRequestException;
import com.gb.dblogger.remoting.exception.RemotingTimeoutException;
import com.gb.dblogger.remoting.exception.RemotingTooMuchRequestException;
import com.gb.dblogger.remoting.netty.NettyRemotingServer;
import com.gb.dblogger.remoting.netty.NettyServerConfig;
import com.gb.dblogger.remoting.netty.ResponseFuture;
import com.gb.dblogger.remoting.protocol.DBLoggerServerStatus;
import com.gb.dblogger.remoting.protocol.DBVersion;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RemotingSerializable;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.remoting.utils.MetricsReporter;
import com.gb.dblogger.server.dubbo.DubboInboundApiProvider;
import com.gb.dblogger.server.model.ClientSessionInfo;
import com.gb.dblogger.server.processor.CliCmdProcessor;
import com.gb.dblogger.server.processor.HeartBeatProcessor;
import com.gb.dblogger.server.processor.SubscribeEventProcessor;
import com.gb.soa.pos.dbsync.api.service.PosOffDbRefreshService;

public class ServerController {
	
	public static final String LOG_SERVER_CONTROLLER = "ServerController";
	
	private static final Logger log = LoggerFactory.getLogger(LOG_SERVER_CONTROLLER);
	
	private static Timer notifyDbLogTimer = MetricsReporter.timer(MetricRegistry.name("perNotifyDbLog"));
	private static Timer totalNotifyDbLogTimer = MetricsReporter.timer(MetricRegistry.name("totalNotifyDbLog"));
	private static Timer notifyBatchUpdTimer = MetricsReporter.timer(MetricRegistry.name("perNotifyBatchUpdTimer"));
	private static Timer totalNotifyBatchUpdTimer = MetricsReporter.timer(MetricRegistry.name("totalNotifyBatchUpdTimer"));
	private static Timer notifyLibUpdTimer = MetricsReporter.timer(MetricRegistry.name("perNotifyLibUpdTimer"));
	private static Timer totalNotifyLibUpdTimer = MetricsReporter.timer(MetricRegistry.name("totalNotifyLibUpdTimer"));
	
	private ClientManager clientManager;
	private NettyRemotingServer nettyRemotingServer;
	private DubboContainer dubboContainer;
	private String dubboZk;
	private String secret;
	private int notifyRetryTimes;
	private PosOffDbRefreshService posOffDbRefreshService;
	
	public PosOffDbRefreshService getPosOffDbRefreshService() {
		return this.posOffDbRefreshService;
	}
	
	public ServerController(NettyServerConfig config) {
		ChannelEventListener channelEventListener = new ClientSessionkeepingService(this);
		nettyRemotingServer = new NettyRemotingServer(config,channelEventListener);
		nettyRemotingServer.registerProcessor(RequestCode.HEART_BEAT, new HeartBeatProcessor(this), null);
		CliCmdProcessor cliCmdProcess = new CliCmdProcessor(this);
		nettyRemotingServer.registerProcessor(RequestCode.GEN_AUTH, cliCmdProcess, null);
		nettyRemotingServer.registerProcessor(RequestCode.DBLOGGER_SERVER_STATUS, cliCmdProcess, null);
		nettyRemotingServer.registerProcessor(RequestCode.SUBSCRIBE_EVENT, new SubscribeEventProcessor(this), null);
		clientManager = new ClientManager(this);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setDubboZk(String zkAddr) {
		this.dubboZk = zkAddr;
		ReferenceConfig reference = new ReferenceConfig();
		reference.setInterface(PosOffDbRefreshService.class);
		RegistryConfig rc = new RegistryConfig();
		rc.setCluster("failover");
		rc.setAddress(this.dubboZk);
		reference.setRegistry(rc);
		reference.setTimeout(10000);
		reference.setRetries(3);
		reference.setProtocol("dubbo");
		this.posOffDbRefreshService = (PosOffDbRefreshService) reference.get();
		dubboContainer = new DubboContainer(dubboZk);//"192.168.26.42:2181"
		dubboContainer.addService(new DubboInboundApiProvider(this));
	}
	
	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}

	public int getNotifyRetryTimes() {
		return notifyRetryTimes;
	}

	public void setNotifyRetryTimes(int notifyRetryTimes) {
		this.notifyRetryTimes = notifyRetryTimes;
	}

	public ClientManager getClientManager() {
		return clientManager;
	}

	public void setClientManager(ClientManager clientManager) {
		this.clientManager = clientManager;
	}
	
	public void start() {
		nettyRemotingServer.start();
		dubboContainer.start();
	}
	
	public void shutdown() {
		nettyRemotingServer.shutdown();
	}
	
	public DBLoggerServerStatus snapshot() {
		DBLoggerServerStatus status = new DBLoggerServerStatus();
		Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.DB_LOG_EVENT);
		Map<String, Object> versions = new HashMap<>();
		int size = 0;
		while(iterator.hasNext()) {
			ClientSessionInfo sessioninfo = iterator.next();
			versions.put(sessioninfo.getClientId()+"["+sessioninfo.getClientVersion()+"]", sessioninfo.dblogversion());
			size ++;
		}
		status.setVersions(versions);
		status.setSessions(size);
		return status;
	}
	
	public DBLoggerServerStatus jettyContainerStatus() {
		DBLoggerServerStatus status = new DBLoggerServerStatus();
		Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.DB_LOG_EVENT);
		Map<String, Object> versions = new HashMap<>();
		int size = 0;
		while(iterator.hasNext()) {
			ClientSessionInfo sessioninfo = iterator.next();
			versions.put(sessioninfo.getClientId()+"["+sessioninfo.getClientVersion()+"]", sessioninfo.jettycontainerStatus() ? "运行中" : "未启动");
			size ++;
		}
		status.setVersions(versions);
		status.setSessions(size);
		return status;
	}
	
	private Iterator<ClientSessionInfo> activeSessions(String eventType){
		return clientManager.observers(eventType);
	}
	
	public void notifyDbLog(final DbChangeEvent dblog) {
		Timer.Context context = totalNotifyDbLogTimer.time();
		try {
			Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.DB_LOG_EVENT);
			while(iterator.hasNext()) {
				ClientSessionInfo session = iterator.next();
				notifyDbLog(dblog,session,null);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			context.stop();
		}
		
	}
	
	public void notifyLibUpd(List<LibUpdEvent> event) {
		Timer.Context context = totalNotifyLibUpdTimer.time();
		log.info("开始处理服务更新通知...");
		try {
			Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.LIB_UPD_EVENT);
			while(iterator.hasNext()) {
				ClientSessionInfo session = iterator.next();
				notifyLibUpd(event,session,null);
			}
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		} finally {
			log.info("服务更新通知处理完成");
			context.stop();
		}
	}
	
	public void notifyLibUpd(List<LibUpdEvent> event, ClientSessionInfo session, InvokeCallback callback) {
		Timer.Context context = notifyLibUpdTimer.time();
		try {
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LIB_UPD, null);
			request.setBody(RemotingSerializable.encode(event));
			if(callback == null) {
				callback = new InvokeCallback() {
					private final AtomicInteger failover = new AtomicInteger(0);
					@Override
					public void operationComplete(ResponseFuture responseFuture) {
						RemotingCommand response = responseFuture.getResponseCommand();
						switch (response.getCode()) {
						case ResponseCode.SUCCESS:
							log.debug("发送服务更新通知成功，稍后由客户端发成功回执");
							context.stop();
							break;
						default:// fail 重试
							if(failover.incrementAndGet() > getNotifyRetryTimes()) {
								log.info("发送服务更新通知{} 重试 {}次 后仍然失败!", event, failover);
								context.stop();
								break;
							}
							log.debug("发送服务更新通知失败{}, 重试",response.getCode());
							context.stop();
							notifyLibUpd(event,session,this);
							break;
						}
					}
				};
			}
			
			int retryTimes = 0;
			for(;;) {
				if(retryTimes > getNotifyRetryTimes()) {
					context.stop();
					break;
				}
				try {
					nettyRemotingServer.invokeAsync(session.getChannel(), request, 10000, callback);
					break;
				} catch (RemotingTooMuchRequestException | RemotingTimeoutException | InterruptedException e) {
					log.error("notifyDbLog exception try it again", e);
				} catch (RemotingSendRequestException e) {
					log.error("notifyDbLog send request error. to avoid concurrent issues, just drop it", e);
					context.stop();
					break;
				}
				retryTimes++;
			}
		} finally {
		}
	}
	
	private void notifyDbLog(final DbChangeEvent dblog,ClientSessionInfo session,InvokeCallback callback) {
		Timer.Context context = notifyDbLogTimer.time();
		try {
			long lastVersion = session.dblogversion();
			if(dblog.getVersion() <= lastVersion) {
				return;
			}
			
			DBVersion dbversion = new DBVersion();
			dbversion.setVersion(lastVersion);
			
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DB_LOG, dbversion);
			request.setBody(JSON.toJSONString(dblog, false).getBytes());
			
			if(callback == null) {
				callback = new InvokeCallback() {
					private final AtomicInteger failover = new AtomicInteger(0);
					@Override
					public void operationComplete(ResponseFuture responseFuture) {
						RemotingCommand response = responseFuture.getResponseCommand();
						switch (response.getCode()) {
						// success 处理下一条
						case ResponseCode.DB_LOG_SUCCESS:
							log.debug("数据同步成功");
							session.dblogversion(dblog.getVersion());
							context.stop();
							break;
						// overdue 发全量更新报文
						case ResponseCode.DB_LOG_OVERDUE:
							log.debug("数据版本过旧，全量更新");
							notifyBatchUpd(session,null);
							context.stop();
							break;
						// fail 重试
						default:
							if(failover.incrementAndGet() > getNotifyRetryTimes()) {
								log.info("数据同步{} 重试 {}次 后仍然失败!", dblog,failover);
								context.stop();
								break;
							}
							log.debug("数据同步失败{},重试",response.getCode());
							context.stop();
							notifyDbLog(dblog,session,this);
							break;
						}
					}
				};
			}
			
			int retryTimes = 0;
			for(;;) {
				if(retryTimes > getNotifyRetryTimes()) {
					context.stop();
					break;
				}
				try {
					nettyRemotingServer.invokeAsync(session.getChannel(), request, 6000, callback);
					break;
				} catch (RemotingTooMuchRequestException | RemotingTimeoutException | InterruptedException e) {
					log.error("notifyDbLog exception try it again", e);
				} catch (RemotingSendRequestException e) {
					log.error("notifyDbLog send request error. to avoid concurrent issues, just drop it", e);
					context.stop();
					break;
				}
				retryTimes++;
			}
		} finally {
		}
	}
	
	public void notifyDbLog(final List<DbChangeEvent> dblogs) {
		Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.DB_LOG_EVENT);
		while(iterator.hasNext()) {
			ClientSessionInfo session = iterator.next();
			List<DbChangeEvent> copy = new ArrayList<>(Arrays.asList(new DbChangeEvent[dblogs.size()]));
			Collections.copy(copy, dblogs);
			notifyDbLog(copy,session,null);
		}
	}
	
	private void notifyDbLog(final List<DbChangeEvent> dblogs, final ClientSessionInfo session, InvokeCallback callback) {
		
		Timer.Context context = notifyDbLogTimer.time();
		
		try {
			if(dblogs.size() <=0) return;
			
			long lastVersion = session.dblogversion();
			DBVersion dbversion = new DBVersion();
			dbversion.setVersion(lastVersion);
			
			DbChangeEvent currentDbChange = null;
//			if(lastVersion == 0) {
//				notifyBatchUpd(session,null);
//				return;
//			}else {
				Iterator<DbChangeEvent> iterator = dblogs.iterator();
				while(iterator.hasNext()) {
					currentDbChange = iterator.next();
					if(currentDbChange.getVersion() <= lastVersion) {
						//丢弃过期的消息
						iterator.remove();
						currentDbChange = null;
					}else {
						break;
					}
				}
//			}
			
			
			if(currentDbChange == null) return;
			
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DB_LOG, dbversion);
			request.setBody(JSON.toJSONString(currentDbChange, false).getBytes());
			
			if(callback == null) {
				final DbChangeEvent tmp = currentDbChange;
				callback = new InvokeCallback() {
					private final List<DbChangeEvent> mydblogs = dblogs;
					private final DbChangeEvent lastDbChange = tmp;
					private final AtomicInteger failover = new AtomicInteger(0);
					@Override
					public void operationComplete(ResponseFuture responseFuture) {
						RemotingCommand response = responseFuture.getResponseCommand();
						switch (response.getCode()) {
						// success 处理下一条
						case ResponseCode.DB_LOG_SUCCESS:
							log.debug("数据同步成功，开始处理下一条更新");
							this.mydblogs.remove(0);
							session.dblogversion(lastDbChange.getVersion());
							context.stop();
							notifyDbLog(this.mydblogs,session,null);
							break;
						// overdue 发全量更新报文
						case ResponseCode.DB_LOG_OVERDUE:
							log.debug("数据版本过旧，全量更新");
							context.stop();
							notifyBatchUpd(session,null);
							break;
						// fail 重试
						default:
							if(failover.incrementAndGet() > getNotifyRetryTimes()) {
								log.info("数据同步{} 重试 {}次 后仍然失败!", lastDbChange,failover);
								context.stop();
								break;
							}
							log.debug("数据同步失败{},重试",response.getCode());
							context.stop();
							notifyDbLog(this.mydblogs,session,this);
							break;
						}
					}
				};
			}
			
			int retryTimes = 0;
			for(;;) {
				if(retryTimes > getNotifyRetryTimes()) {
					context.stop();
					break;
				}
				try {
					nettyRemotingServer.invokeAsync(session.getChannel(), request, 6000, callback);
					break;
				} catch (RemotingTooMuchRequestException | RemotingTimeoutException | InterruptedException e) {
					log.error("notifyDbLog exception try it again", e);
				} catch (RemotingSendRequestException e) {
					log.error("notifyDbLog send request error. to avoid concurrent issues, just drop it", e);
					context.stop();
					break;
				}
				retryTimes ++ ;
			}
		} finally {
		}
	}
	
	@Deprecated
	private void notifyBatchUpd(ClientSessionInfo session, InvokeCallback callback) {
		Timer.Context context = notifyBatchUpdTimer.time();
		try {
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DB_OVERDUE, null);
			if(callback == null) {
				callback = new InvokeCallback() {
					
					private AtomicInteger failover = new AtomicInteger(0);
					
					@Override
					public void operationComplete(ResponseFuture responseFuture) {
						RemotingCommand response = responseFuture.getResponseCommand();
						//从客户端响应拿到当前更新到的最新版本 号，存入session中
						if(response.getCode() == ResponseCode.SUCCESS) {
							String version = response.getExtFields().get("version");
							session.dblogversion(Long.parseLong(version));
							context.stop();
						}else {
							if(this.failover.incrementAndGet() > getNotifyRetryTimes()) {
								log.info("数据批量同步,重试 {}次 后仍然失败!",failover);
								context.stop();
								return;
							}
							context.stop();
							notifyBatchUpd(session,this);
						}
					}
				};
			}
			
			int retryTimes = 0;
			for(;;) {
				if(retryTimes > getNotifyRetryTimes()) {
					context.stop();
					break;
				}
				try {
					nettyRemotingServer.invokeAsync(session.getChannel(), request, 10000, callback);
					break;
				} catch (RemotingTooMuchRequestException | RemotingTimeoutException | InterruptedException | RemotingSendRequestException e) {
					log.error("notifyBatchUpd exception, try it again", e);
				} 
				retryTimes ++;
			}
		} finally {
		}
	}
	
	public void notifyAlterTable(String sql) {
		try {
			Iterator<ClientSessionInfo> iterator = activeSessions(RequestCode.TABLE_ALTER_EVENT);
			while(iterator.hasNext()) {
				ClientSessionInfo session = iterator.next();
				notifyAlterTable(session,sql,null);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			
		}
		
	}
	
	private void notifyAlterTable(ClientSessionInfo session, String sql, InvokeCallback callback) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.TABLE_ALTER, null);
		request.setBody(sql.getBytes(Charset.forName("utf-8")));
		
		if(callback == null) {
			callback = new InvokeCallback() {
				private final AtomicInteger failover = new AtomicInteger(0);
				@Override
				public void operationComplete(ResponseFuture responseFuture) {
					RemotingCommand response = responseFuture.getResponseCommand();
					switch (response.getCode()) {
					// success 
					case ResponseCode.DB_ALTER_TABLE_SUCCESS:
						log.debug("ALTER TABLE成功");
						break;
					// fail 重试
					default:
						if(failover.incrementAndGet() > getNotifyRetryTimes()) {
							log.info("ALTER TABLE同步{} 重试 {}次 后仍然失败!", sql,failover);
							break;
						}
						log.debug("ALTER TABLE同步失败{},重试",response.getCode());
						notifyAlterTable(session,sql,this);
						break;
					}
				}
			};
		}
		
		int retryTimes = 0;
		for(;;) {
			if(retryTimes > getNotifyRetryTimes()) {
				break;
			}
			try {
				nettyRemotingServer.invokeAsync(session.getChannel(), request, 10000, callback);
				break;
			} catch (RemotingTooMuchRequestException | RemotingTimeoutException | InterruptedException e) {
				log.error("notifyAlterTable exception try it again", e);
			} catch (RemotingSendRequestException e) {
				log.error("notifyAlterTable send request error. to avoid concurrent issues, just drop it", e);
				break;
			}
			retryTimes ++ ;
		}
		
	}
	
	public static void main(String[] args) {
		Timer.Context context = notifyDbLogTimer.time();
		context.stop();
		context.stop();
		NettyServerConfig config = new NettyServerConfig();
		config.setListenPort(8099);
		config.setServerCallbackExecutorThreads(64);
		config.setServerAsyncSemaphoreValue(1024);
		ServerController controller = new ServerController(config);
		controller.setDubboZk("192.168.26.77:2181");
		controller.start();
		long version = 0;
		for(;;) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for(int i=0;i<20;i++) {
				DbChangeEvent changeEvent = new DbChangeEvent();
				changeEvent.setDatabase("test_database");
				changeEvent.setTable("test_table");
				changeEvent.setVersion(version++);
				controller.notifyDbLog(changeEvent);
			}
			
			
		}
	}
	
	
}
