package com.gb.dbchange.common.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemotingCommand {
	
	private static final Logger log = LoggerFactory.getLogger(RemotingCommand.class);
	
	private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
	
	private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;
	
	private static AtomicInteger requestId = new AtomicInteger(0);
	
	

	private int code;
    private int version = 0;
    private int opaque = requestId.getAndIncrement();
    private int flag = 0;
    
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;
    private transient byte[] body;
	
	public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        return RemotingCommandType.REQUEST_COMMAND;
    }

    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }
    
    
    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }
    
    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }
    
    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }
    
    private byte[] headerEncode() {
        return RemotingSerializable.encode(this);
    }
    
    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            default:
                break;
        }

        return null;
    }
    
    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
    
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        int length = byteBuffer.limit();
        int oriHeaderLen = byteBuffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }
    
    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }
    
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }
    
    public byte[] getBody() {
    	return this.body;
    }
}
