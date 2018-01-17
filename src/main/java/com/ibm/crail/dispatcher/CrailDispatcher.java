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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailLocationClass;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;
import com.ibm.narpc.NaRPCService;

public class CrailDispatcher implements NaRPCService<PutGetRequest, PutGetResponse>{
	private static final Logger LOG = LoggerFactory.getLogger("com.ibm.crail.dispatcher");
	
	private NaRPCServerGroup<PutGetRequest, PutGetResponse> serverGroup;
	private NaRPCServerEndpoint<PutGetRequest, PutGetResponse> serverEndpoint;
	private CrailFS crailFS;
	private CrailBuffer buffer;
	
	public CrailDispatcher(InetSocketAddress address) throws Exception{
		this.serverGroup = new NaRPCServerGroup<PutGetRequest, PutGetResponse>(this, 16, 1024, true);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		serverEndpoint.bind(address);	
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
		PutGetResponse ret = PutGetResponse.ERROR;
		long start = System.nanoTime();
		try {
			if (request.getType() == PutGetRequest.CMD_PUT){
				ret = put(request.getSrcFile(), request.getDstFile());
			} else if (request.getType() == PutGetRequest.CMD_GET){
				ret = get(request.getSrcFile(), request.getDstFile());
			} else if (request.getType() == PutGetRequest.CMD_DEL){
				ret = del(request.getSrcFile());
			} else if (request.getType() == PutGetRequest.CMD_CREATE_DIR){
				ret = create_dir(request.getSrcFile());
			} 
		} catch(Exception e){
		}
		long end = System.nanoTime();
		long exeuctionTime = (end - start)/1000;	
		LOG.info("executionTime " + exeuctionTime);
		return ret;
	}
	
	private PutGetResponse put(String srcFile, String dstFile) throws Exception {
		LOG.info("PUT, srcFile " + srcFile + ", dstFile " + dstFile);
		
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
		LOG.info("GET, srcFile " + srcFile + ", dstFile " + dstFile);
		
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
	
	private PutGetResponse del(String srcFile) throws Exception{
		LOG.info("DEL, srcFile " + srcFile);
		
		crailFS.delete(srcFile, true).get();
		
		return PutGetResponse.OK;
	}	

	private PutGetResponse create_dir(String srcFile) throws Exception{
		LOG.info("CREATE DIR, srcFile " + srcFile);
		
		crailFS.create(srcFile, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT).get();
		
		return PutGetResponse.OK;
	}	

	public void run() throws Exception{
		while (true) {
			NaRPCServerChannel endpoint = serverEndpoint.accept();
		}		
	}
	
	public static void main(String args[]) throws Exception{
		int threadCount = 1;
		InetSocketAddress address = new InetSocketAddress("localhost", 2345);
		
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
		
		CrailDispatcher dispatcher = new CrailDispatcher(address);
		dispatcher.run();
	}
}
