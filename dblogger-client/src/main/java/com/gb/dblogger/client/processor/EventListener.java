package com.gb.dblogger.client.processor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.gb.dblogger.remoting.netty.NettyRequestProcessor;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.gb.dblogger.remoting.utils.MetricsReporter;

import io.netty.channel.ChannelHandlerContext;

/**
 * 通用业务处理类
 * @author xuelong.chen
 *
 * @param <T>
 */
public abstract class EventListener implements NettyRequestProcessor {
	
	private final Counter successCounter = MetricsReporter.register(this.getClass().getName()+"Success", new Counter());
	private final Counter totalCounter = MetricsReporter.register(this.getClass().getName()+"Total", new Counter());
	private final Timer dealTimer = MetricsReporter.timer(this.getClass().getName()+"Timer");
	
	protected abstract Object decode(final RemotingCommand request) throws Exception;

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		totalCounter.inc();
		Timer.Context context = dealTimer.time();
		try {
			Object message = decode(request);
			if(this.deal(message, request, response)) {
				response.setCode(ResponseCode.SUCCESS);
				response.setRemark("deal successfully");
				successCounter.inc();
			}
		} catch (Exception e) {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("System Error:"+e.getMessage());
		} finally {
			context.stop();
		}
		return response;
	}
	
	protected abstract boolean deal(Object t,final RemotingCommand request, final RemotingCommand response) throws Exception;

	@Override
	public boolean rejectRequest() {
		return false;
	}

}
