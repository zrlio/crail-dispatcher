package com.ibm.crail.dispatcher;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.narpc.NaRPCMessage;

public class PutGetResponse implements NaRPCMessage {
	public static final PutGetResponse ERROR = new PutGetResponse((short)-1);
	public static final PutGetResponse OK = new PutGetResponse((short)0);
	
	private short error;
	
	public PutGetResponse(short error){
		this.error = error;
	}
	
	@Override
	public void update(ByteBuffer buffer) throws IOException {
		this.error = buffer.getShort();
	}

	@Override
	public int write(ByteBuffer buffer) throws IOException {
		buffer.putShort(error);
		return Short.BYTES;
	}

}
