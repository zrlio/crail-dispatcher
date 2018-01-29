package com.ibm.crail.dispatcher;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class CrailDispatchBenchmark {
	private static final Logger LOG = LoggerFactory.getLogger("com.ibm.crail.dispatcher");
	
	public static void main(String args[]) throws Exception{
		short type = 0;
		String srcFile = "";
		String dstFile = "";
		int loop = 1;
		InetSocketAddress address = new InetSocketAddress("localhost", 2345);
		
		if (args != null) {
			Option typeOption = Option.builder("t").desc("type").hasArg().build();
			Option srcOption = Option.builder("s").desc("src file").hasArg().build();
			Option dstOption = Option.builder("d").desc("dst file").hasArg().build();
			Option loopOption = Option.builder("k").desc("loop file").hasArg().build();
			
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(srcOption);
			options.addOption(dstOption);
			options.addOption(loopOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(typeOption.getOpt())) {
					String _type = line.getOptionValue(typeOption.getOpt());
					if (_type.equalsIgnoreCase("put")){
						type = PutGetRequest.CMD_PUT;
					} else if (_type.equalsIgnoreCase("get")){
						type = PutGetRequest.CMD_GET;
					} else if (_type.equalsIgnoreCase("del")){
						type = PutGetRequest.CMD_DEL;
					}
				}	
				if (line.hasOption(srcOption.getOpt())) {
					srcFile = line.getOptionValue(srcOption.getOpt());
				}
				if (line.hasOption(dstOption.getOpt())) {
					dstFile = line.getOptionValue(dstOption.getOpt());
				}	
				if (line.hasOption(loopOption.getOpt())) {
					loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
				}					
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}
		
		NaRPCClientGroup<PutGetRequest, PutGetResponse> clientGroup = new NaRPCClientGroup<PutGetRequest, PutGetResponse>(16, 512, true);
		NaRPCEndpoint<PutGetRequest, PutGetResponse> endpoint = clientGroup.createEndpoint();
		endpoint.connect(address);	
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			String actualDstFile = dstFile + i;
			PutGetRequest request = new PutGetRequest(type, srcFile, actualDstFile);
			PutGetResponse response = new PutGetResponse((short) 0);
			endpoint.issueRequest(request, response).get();		
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double latency = 0.0;
		double ops = (double) loop;
		if (executionTime > 0) {
			latency = 1000000.0 * executionTime / ops;
		}
		LOG.info("execution time " + executionTime);
		LOG.info("ops " + ops);
		LOG.info("latency " + latency);			
	}
}
