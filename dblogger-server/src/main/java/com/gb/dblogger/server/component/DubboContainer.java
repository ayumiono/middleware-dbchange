package com.gb.dblogger.server.component;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;

public class DubboContainer {
	
	private final List<Object> apis = new ArrayList<>();
	
	private String zkAddr;
	
	public DubboContainer(String zkAddr) {
		this.zkAddr = zkAddr;
	}
	
	public void addService(Object ref) {
		this.apis.add(ref);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void start() {
		ApplicationConfig application = new ApplicationConfig();
		application.setName("dblogger");
		RegistryConfig registry = new RegistryConfig();
		registry.setProtocol("zookeeper");
		registry.setAddress(zkAddr);
		for(Object api : apis) {
			Class<?>[] interfaces = api.getClass().getInterfaces();
			ServiceConfig serviceConfig = new ServiceConfig();
			serviceConfig.setProtocol(new ProtocolConfig("dubbo"));
			serviceConfig.setApplication(application);
			serviceConfig.setRegistry(registry);
			serviceConfig.setInterface(interfaces[0]);
			serviceConfig.setRef(api);
			serviceConfig.export();
		}
	}
}
