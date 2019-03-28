package com.gb.dbchange.server.boot;

import com.gb.dbchange.server.component.ClientSessionHoldService;
import com.gb.dbchange.server.component.RocketMQConsumer;
import com.gb.dbchange.server.remoting.NettyRemotingServer;

/**
 * 服务端启动类
 * 
 * @author xuelong.chen
 *
 */
public class BootStrap {
	
	private ClientSessionHoldService sessionHoldService;
	
	private RocketMQConsumer mqConsumer;
	
	private NettyRemotingServer nettyServer;
	
	
	
	public static void main(String[] args) {
		
	}
}
