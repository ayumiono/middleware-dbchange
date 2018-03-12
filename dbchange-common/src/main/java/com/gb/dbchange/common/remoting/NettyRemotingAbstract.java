package com.gb.dbchange.common.remoting;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dbchange.common.component.ChannelEventListener;
import com.gb.dbchange.common.component.ServiceThread;
import com.gb.dbchange.common.model.NettyEvent;
import com.gb.dbchange.common.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

public abstract class NettyRemotingAbstract implements RemotingService {
	
	/**
     * Remoting logger instance.
     */
    private static final Logger log = LoggerFactory.getLogger(NettyRemotingAbstract.class);
	
	 /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected SslContext sslContext;
    
    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();
    
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }
    
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }
    
    /**
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
    	
    }
    
    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
    	
    }
    
    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();
    
    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
