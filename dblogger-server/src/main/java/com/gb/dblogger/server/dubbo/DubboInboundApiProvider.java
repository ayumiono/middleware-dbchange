package com.gb.dblogger.server.dubbo;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.gb.dblogger.common.dubbo.DubboInboundApi;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.common.protocol.DbChangeEventResponse;
import com.gb.dblogger.common.protocol.DbTableColumnChangeEventRequest;
import com.gb.dblogger.common.protocol.DbTableColumnChangeEventResponse;
import com.gb.dblogger.common.protocol.JavaProjectdownloadResponse;
import com.gb.dblogger.common.protocol.LibUpdEvent;
import com.gb.dblogger.server.component.ServerController;

/**
 * 为了防止dubbo超时，都放到线程池中去调controller
 * @author xuelong.chen
 *
 */
public class DubboInboundApiProvider implements DubboInboundApi {
	
	private ServerController controller;
	
	private ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	
	private static ConcurrentHashMap<String, LibUpdEvent> libVersionCache = new ConcurrentHashMap<>();
	
	public static Collection<LibUpdEvent> latestLibs(){
		return libVersionCache.values();
	}
	
	public static void removeLibUpdCache(String projectName) {
		libVersionCache.remove(projectName);
	}
	
	public static void clearLibUpdCache() {
		libVersionCache.clear();
	}
	
	public DubboInboundApiProvider(ServerController controller) {
		this.controller = controller;
	}

	@Override
	public DbChangeEventResponse notify(DbChangeEvent event) {
		
		pool.execute(new Runnable() {
			@Override
			public void run() {
				controller.notifyDbLog(event);
			}
		});
		return new DbChangeEventResponse();
	}

	@Override
	public DbTableColumnChangeEventResponse changeDbTableColumn(DbTableColumnChangeEventRequest req) {
		
		pool.execute(new Runnable() {
			@Override
			public void run() {
				controller.notifyAlterTable(req.getTableColumnSql());
			}
		});
		
		return new DbTableColumnChangeEventResponse();
	}

	@Override
	public JavaProjectdownloadResponse downloadJavaProject(LibUpdEvent arg0) {
		
		//缓存最新的jar更新记录
		String projectName = arg0.getProjectName();
		String version = arg0.getVersion();
		if(libVersionCache.get(projectName) == null) {
			libVersionCache.put(projectName, arg0);
		}else {
			LibUpdEvent old = libVersionCache.get(projectName);
			if(Integer.parseInt(old.getVersion()) < Integer.parseInt(version)) {
				libVersionCache.put(projectName, arg0);
			}
		}
		
		pool.execute(new Runnable() {
			@Override
			public void run() {
				controller.notifyLibUpd(Collections.singletonList(arg0));
			}
		});
		
		return new JavaProjectdownloadResponse();
	}
}
