package com.ibm.crail.dispatcher;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
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
	private CrailBuffer[] bufferList;
	private ArrayBlockingQueue<CrailBuffer> pendingBuffers;
	private ArrayBlockingQueue<Future<CrailResult>> pendingFutures;
	
	public CrailDispatcher(InetSocketAddress address, int bufferCount) throws Exception{
		LOG.info("starting new crail dispatcher, address " + address.toString() + ", bufferCount " + bufferCount);
		this.serverGroup = new NaRPCServerGroup<PutGetRequest, PutGetResponse>(this, 16, 1024, true);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		serverEndpoint.bind(address);	
		CrailConfiguration conf = new CrailConfiguration();
		this.crailFS = CrailFS.newInstance(conf);
		this.bufferList = new CrailBuffer[bufferCount];
		this.pendingBuffers = new ArrayBlockingQueue<CrailBuffer>(bufferCount);
		this.pendingFutures = new ArrayBlockingQueue<Future<CrailResult>>(bufferCount);
		for (int i = 0; i < bufferCount; i++){
			CrailBuffer buffer = crailFS.allocateBuffer();
			bufferList[i] = buffer;
		}
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
			LOG.info("Error, exception message " + e.getMessage());
			e.printStackTrace();
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
		
		boolean isDone = false;
		for (int i = 0; i < bufferList.length; i++){
			CrailBuffer buffer = bufferList[i];
			buffer.clear();
			if (srcChannel.read(buffer.getByteBuffer()) < 0){
				isDone = true;
				break;
			} 
			
			buffer.flip();
			Future<CrailResult> future = dstChannel.write(buffer);
			pendingBuffers.add(buffer);
			pendingFutures.add(future);
		}
		
		while(!isDone){
			Future<CrailResult> future = pendingFutures.poll();
			future.get();
			CrailBuffer buffer = pendingBuffers.poll();
			buffer.clear();
			
			if (srcChannel.read(buffer.getByteBuffer()) < 0){
				isDone = true;
				break;
			} 
			
			buffer.flip();
			future = dstChannel.write(buffer);
			pendingBuffers.add(buffer);
			pendingFutures.add(future);			
		}
		
		while(!pendingFutures.isEmpty()){
			Future<CrailResult> future = pendingFutures.poll();
			future.get();
			CrailBuffer buffer = pendingBuffers.poll();
		}
		
		if (!pendingFutures.isEmpty() || !pendingBuffers.isEmpty()){
			throw new Exception("Pending operations left");
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
		
		boolean isDone = false;
		for (int i = 0; i < bufferList.length; i++){
			CrailBuffer buffer = bufferList[i];
			buffer.clear();
			Future<CrailResult> future = srcChannel.read(buffer);
			if (future == null){
				isDone = true;
				break;
			} 
			
			pendingFutures.add(future);
			pendingBuffers.add(buffer);
		}
		
		while(!isDone){
			Future<CrailResult> future = pendingFutures.poll();
			future.get();
			CrailBuffer buffer = pendingBuffers.poll();
			buffer.flip();
			dstChannel.write(buffer.getByteBuffer());

			buffer.clear();
			future = srcChannel.read(buffer);
			if (future == null){
				isDone = true;
				break;
			} 
			
			pendingFutures.add(future);
			pendingBuffers.add(buffer);			
		}
		
		while(!pendingFutures.isEmpty()){
			Future<CrailResult> future = pendingFutures.poll();
			future.get();
			CrailBuffer buffer = pendingBuffers.poll();
			buffer.flip();
			dstChannel.write(buffer.getByteBuffer());
		}
		
		if (!pendingFutures.isEmpty() || !pendingBuffers.isEmpty()){
			throw new Exception("Pending operations left");
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
		int bufferCount = 1;
		InetSocketAddress address = new InetSocketAddress("localhost", 2345);
		
		if (args != null) {
			Option threadOption = Option.builder("n").desc("number of threads").hasArg().build();
			Option bufferOption = Option.builder("d").desc("number of buffers").hasArg().build();
			Options options = new Options();
			options.addOption(threadOption);
			options.addOption(bufferOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(threadOption.getOpt())) {
					threadCount = Integer.parseInt(line.getOptionValue(threadOption.getOpt()));
				}	
				if (line.hasOption(bufferOption.getOpt())) {
					bufferCount = Integer.parseInt(line.getOptionValue(bufferOption.getOpt()));
				}				
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}
		
		CrailDispatcher dispatcher = new CrailDispatcher(address, bufferCount);
		dispatcher.run();
	}
}
