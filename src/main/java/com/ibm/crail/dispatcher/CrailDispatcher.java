package com.ibm.crail.dispatcher;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.Future;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailLocationClass;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;
import com.ibm.narpc.NaRPCService;

public class CrailDispatcher implements NaRPCService<PutGetRequest, PutGetResponse>{
	private NaRPCServerGroup<PutGetRequest, PutGetResponse> serverGroup;
	private NaRPCServerEndpoint<PutGetRequest, PutGetResponse> serverEndpoint;
	private CrailFS crailFS;
	private CrailBuffer buffer;
	
	public CrailDispatcher() throws Exception{
		this.serverGroup = new NaRPCServerGroup<PutGetRequest, PutGetResponse>(this, 16, 1024, true);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
		serverEndpoint.bind(inetSocketAddress);	
		CrailConfiguration conf = new CrailConfiguration();
		this.crailFS = CrailFS.newInstance(conf);
		buffer = crailFS.allocateBuffer();
	}
	
	@Override
	public PutGetRequest createRequest() {
		return new PutGetRequest();
	}

	@Override
	public PutGetResponse processRequest(PutGetRequest request) {
		try {
			if (request.getType() == PutGetRequest.CMD_PUT){
				return put(request.getSrcFile(), request.getDstFile());
			} else if (request.getType() == PutGetRequest.CMD_GET){
				return get(request.getSrcFile(), request.getDstFile());
			} else {
				return PutGetResponse.ERROR;
			}
		} catch(Exception e){
			return PutGetResponse.ERROR;
		}
	}
	
	private PutGetResponse put(String srcFile, String dstFile) throws Exception {
		RandomAccessFile _srcFile     = new RandomAccessFile(srcFile, "rw");
		FileChannel srcChannel = _srcFile.getChannel();		
		CrailOutputStream dstChannel = crailFS.create(dstFile, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT).get().asFile().getDirectOutputStream(0);
		
		buffer.clear();
		while(srcChannel.read(buffer.getByteBuffer()) > 0){
			buffer.flip();
			dstChannel.write(buffer).get();
			
			buffer.clear();
		}
		
		srcChannel.close();
		dstChannel.close();
		
		return PutGetResponse.OK;
	}
	
	private PutGetResponse get(String srcFile, String dstFile) throws Exception{
		CrailInputStream srcChannel = crailFS.lookup(srcFile).get().asFile().getDirectInputStream(0);
		RandomAccessFile _dstFile     = new RandomAccessFile(dstFile, "rw");
		FileChannel dstChannel = _dstFile.getChannel();
		
		buffer.clear();
		Future<CrailResult> future = srcChannel.read(buffer);
		while(future != null){
			future.get();
			buffer.flip();
			dstChannel.write(buffer.getByteBuffer());
			
			buffer.clear();
			future = srcChannel.read(buffer);			
		}
		
		srcChannel.close();
		dstChannel.close();
		
		return PutGetResponse.OK;
	}	

	public void run() throws Exception{
		while (true) {
			NaRPCServerChannel endpoint = serverEndpoint.accept();
		}		
	}
	
	public static void main(String args[]) throws Exception{
		int threadCount = 1;
		
		if (args != null) {
			Option threadOption = Option.builder("n").desc("number of threads").hasArg().build();
			Options options = new Options();
			options.addOption(threadOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(threadOption.getOpt())) {
					threadCount = Integer.parseInt(line.getOptionValue(threadOption.getOpt()));
				}	
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}
		
		CrailDispatcher dispatcher = new CrailDispatcher();
		dispatcher.run();
	}
}
