package com.gb.dblogger.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.client.component.DbChangeEventListener;
import com.gb.dblogger.client.impl.DBLoggerClientApiImpl;
import com.gb.dblogger.client.processor.DbBatchUpdProcessor;
import com.gb.dblogger.client.processor.DbChangeEventProcessor;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.remoting.RemotingClient;
import com.gb.dblogger.remoting.exception.DBChangeEventException;
import com.gb.dblogger.remoting.exception.HeartBeatException;
import com.gb.dblogger.remoting.exception.RemotingException;
import com.gb.dblogger.remoting.netty.NettyClientConfig;
import com.gb.dblogger.remoting.netty.NettyRemotingClient;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.HeartBeatData;
import com.gb.dblogger.remoting.protocol.RequestCode;

public class DbLoggerClient extends ClientConfig {
	
	public static final String DBLOGGER_CLIENT = "DbLoggerClient";
	
	public static final Logger log = LoggerFactory.getLogger(DBLOGGER_CLIENT);
	
	private final DBLoggerClientApiImpl clientApi;
	
	private final RemotingClient remotingClient;
	
	private volatile Long dbVersion;
	
	private final ReentrantLock lockHeartbeat = new ReentrantLock();
	
	private boolean needResubscribe = false;
	
	public RemotingClient getRemotingClient() {
		return remotingClient;
	}
	
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "DbLoggerClientScheduledThread");
        }
    });
	
	public DbLoggerClient() {
		NettyClientConfig nettyClientConfig = new NettyClientConfig();//TODO ClientConfig to NettyClientConfig
		this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
		this.clientApi = new DBLoggerClientApiImpl(this);
	}
	
	public Long getDbVersion() {
		return dbVersion;
	}

	public void setDbVersion(Long dbVersion) {
		this.dbVersion = dbVersion;
	}

	public void registerProcessor(int requestCode, NettyRequestProcessor processor) {
		this.remotingClient.registerProcessor(requestCode, processor, null);
	}

	public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executors) {
		this.remotingClient.registerProcessor(requestCode, processor, executors);
	}
	
	public void registerDbChangeEventProcessor(DbChangeEventListener listener) {
		DbChangeEventProcessor processor = new DbChangeEventProcessor(listener,this);
		this.registerProcessor(RequestCode.DB_LOG, processor);
		this.registerProcessor(RequestCode.TABLE_ALTER, processor);
	}
	
	public void start() {
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				if(lockHeartbeat.tryLock()) {
					try {
						HeartBeatData heartBeatData  = null;
						try {
							heartBeatData = generateHeartBeatData();
						} catch(Exception e) {
							log.error("生成心跳数据失败",e);
							return;
						} 
						
						try {
							clientApi.heartBeat(heartBeatData);
						} catch (RemotingException | InterruptedException | HeartBeatException e) {
							log.error("心跳发送失败");
							needResubscribe = true;
							return;
						} catch (Exception e) {
							log.error("心跳发送失败，未知原因", e);
							needResubscribe = true;
							return;
						}
						
						try {
							if(needResubscribe) {
								log.info("重新订阅");
								try {
									if(!sendSubscription()) {
										log.error("重新订阅失败");
										return;
									}
									needResubscribe = false;
								} catch (Exception e) {
									log.error("重新订阅失败",e);
								}
							}
						} catch (Exception e) {
							log.error("重新订阅失败");
						}
					} finally {
						lockHeartbeat.unlock();
					}
				}else {
					log.info("lock heartbeat, but failed");
				}
			}
		}, 1000, this.getHeartBeatInterval(), TimeUnit.MILLISECONDS);
		remotingClient.start();
		try {
			if(!sendSubscription()) {
				log.error("订阅失败");
//				this.shutdown(); 解决[如果项目初始启动时碰到断网问题]
				this.needResubscribe = true;
			}
		} catch (Exception e) {
			log.error("订阅失败");
//			this.shutdown();
			this.needResubscribe = true;
		}
	}
	
	private boolean sendSubscription() throws Exception {
		return this.clientApi.subscribe(new String[] {"DB_LOG","TABLE_ALTER"});
	}
	
	public void shutdown() {
		try {
			if(lockHeartbeat.tryLock(this.getHeartBeatInterval(), TimeUnit.MILLISECONDS)) {
				try {
					this.scheduledExecutorService.shutdown();
					this.remotingClient.shutdown();
				} finally {
					lockHeartbeat.unlock();
				}
			}
		} catch (Exception e) {
			log.error("shutdown failed.", e);
		}
	}
	
	private HeartBeatData generateHeartBeatData() {
		HeartBeatData heartBeatData = new HeartBeatData();
		return heartBeatData;
	}
	
	public static void main(String[] args) {
		for(int i=0;i<3000;i++) {
			DbLoggerClient client = new DbLoggerClient();
			client.setAddr("192.168.26.183:8099");
			client.setClientId("test-client-"+i);
			client.setAppId("9798A40CD7865E9E");
			client.setSign("8E367C2FEDD188EB7C386ED4C6E1DB6B");//right sign
			client.registerProcessor(RequestCode.DB_LOG, new DbChangeEventProcessor(new DbChangeEventListener() {
				
				@Override
				public long deal(DbChangeEvent dbLog) throws DBChangeEventException {
					log.info(dbLog.toString());
					return 0L;
				}

				@Override
				public boolean alterTabel(String sql) {
					log.info(sql);
					return false;
				}
			},client));
			client.registerProcessor(RequestCode.DB_OVERDUE, new DbBatchUpdProcessor(client));
			client.start();
		}
	}
	
//	/**
//	 * epoll bug fixed in netty
//	 * @param args
//	 * @throws InterruptedException
//	 */
//	public static void main(String[] args) throws InterruptedException {
//		final Semaphore semaphore = new Semaphore(0);
//		final AtomicReference<Thread> thread = new AtomicReference<Thread>();
//		EventLoopGroup workerGroup = new NioEventLoopGroup();
//		workerGroup.next().execute(new Runnable() {
//		    @Override public void run() {
//		        thread.set(Thread.currentThread());
//		        semaphore.release();
//		    }
//		});
//		semaphore.acquire();
//		thread.get().interrupt();
//	}
}
