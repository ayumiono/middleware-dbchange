package com.gb.dbchange.common.remoting;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dbchange.common.protocol.RemotingCommand;
import com.gb.dbchange.common.utils.RemotingHelper;
import com.gb.dbchange.common.utils.RemotingUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
	
	private static final Logger log = LoggerFactory.getLogger(NettyEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
		try {
            ByteBuffer header = remotingCommand.encodeHeader();
            out.writeBytes(header);
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            RemotingUtil.closeChannel(ctx.channel());
        }
	}

}
