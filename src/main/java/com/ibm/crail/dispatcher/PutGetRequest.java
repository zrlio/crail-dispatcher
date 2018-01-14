package com.ibm.crail.dispatcher;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.narpc.NaRPCMessage;

public class PutGetRequest implements NaRPCMessage {
	public static final short CMD_PUT = 0;
	public static final short CMD_GET = 1;
	
	private short type;
	private int srcSize;
	private String srcFile;
	private int dstSize;
	private String dstFile;
	
	public PutGetRequest(short type, String srcFile, String dstFile){
		this.type = type;
		this.srcFile = srcFile;
		this.dstFile = dstFile;
	}
	
	public PutGetRequest(){
	}
	
	@Override
	public void update(ByteBuffer buffer) throws IOException {
		this.type = buffer.getShort();
		this.srcSize = buffer.getInt();
		byte[] srcBuffer = new byte[srcSize];
		buffer.get(srcBuffer);
		this.srcFile = new String(srcBuffer);
		this.dstSize = buffer.getInt();
		byte[] dstBuffer = new byte[dstSize];
		buffer.get(dstBuffer);
		this.dstFile = new String(dstBuffer);
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putShort(type);
		byte srcBuffer[] = srcFile.getBytes();
		buffer.putInt(srcBuffer.length);
		buffer.put(srcBuffer);
		byte dstBuffer[] = dstFile.getBytes();
		buffer.putInt(dstBuffer.length);
		buffer.put(dstBuffer);		
		return 0;
	}

	public short getType() {
		return type;
	}

	public String getSrcFile() {
		return srcFile;
	}

	public String getDstFile() {
		return dstFile;
	}
}
