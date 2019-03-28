package com.gb.dblogger.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.client.impl.CommonClientApiImpl;
import com.gb.dblogger.client.processor.EventListener;
import com.gb.dblogger.remoting.RemotingClient;
import com.gb.dblogger.remoting.exception.HeartBeatException;
import com.gb.dblogger.remoting.exception.RemotingException;
import com.gb.dblogger.remoting.protocol.HeartBeatData;

public abstract class AbstractCommonClient extends ClientConfig {
	
	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private RemotingClient client;

	private CommonClientApiImpl clientApi;

	private final ReentrantLock lockHeartbeat = new ReentrantLock();
	
	private Map<Integer, EventListener> listenerCache = new HashMap<>();
	
	protected Set<String> subscriptions = new HashSet<>();
	
	private boolean needResubscribe = false;

	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "DbLoggerClientScheduledThread");
				}
			});

	public AbstractCommonClient(RemotingClient client) {
		this.client = client;
		this.clientApi = new CommonClientApiImpl(this);
	}

	/**
	 * 订阅事件
	 * @param eventType 事件类型
	 * @param requestCode	事件代码
	 * @param listener	回调函数
	 */
	public void subscribe(String eventType, int requestCode, EventListener listener) {
		this.client.registerProcessor(requestCode, listener, null);
		this.subscriptions.add(eventType);
	}
	
	public void subscribe(String... eventTypes) {
		this.subscriptions.addAll(Arrays.asList(eventTypes));
	}
	
	public void registerEventListener(int requestCode, EventListener listener) {
		this.listenerCache.put(requestCode, listener);
		this.client.registerProcessor(requestCode, listener, null);
	}
	
	protected abstract HeartBeatData generateHeartBeatData();
	
	public RemotingClient getRemotingClient() {
		return client;
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
		client.start();
		try {
			if(!sendSubscription()) {
				log.error("订阅失败");
				this.needResubscribe = true;
//				this.shutdown();
			}
		} catch (Exception e) {
			log.error("订阅失败");
//			this.shutdown();
			this.needResubscribe = true;
		}
	}
	
	public void shutdown() {
		try {
			if(lockHeartbeat.tryLock(this.getHeartBeatInterval(), TimeUnit.MILLISECONDS)) {
				try {
					this.scheduledExecutorService.shutdown();
					this.client.shutdown();
				} finally {
					lockHeartbeat.unlock();
				}
			}
		} catch (Exception e) {
			log.error("shutdown failed.", e);
		}
	}
	
	private boolean sendSubscription() throws Exception {
		return this.clientApi.subscribe(this.subscriptions.toArray(new String[] {}));
	}
}
