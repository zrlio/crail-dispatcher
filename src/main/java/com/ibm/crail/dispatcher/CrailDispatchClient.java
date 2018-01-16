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
import com.ibm.narpc.NaRPCGroup;

public class CrailDispatchClient {
	private static final Logger LOG = LoggerFactory.getLogger("com.ibm.crail.dispatcher");
	
	public static void main(String args[]) throws Exception{
		short type = 0;
		String srcFile = "";
		String dstFile = "";
		InetSocketAddress address = new InetSocketAddress("localhost", 2345);
		
		if (args != null) {
			Option typeOption = Option.builder("t").desc("type").hasArg().build();
			Option srcOption = Option.builder("s").desc("src file").hasArg().build();
			Option dstOption = Option.builder("d").desc("dst file").hasArg().build();
			
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(srcOption);
			options.addOption(dstOption);
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
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}
		
		NaRPCClientGroup<PutGetRequest, PutGetResponse> clientGroup = new NaRPCClientGroup<PutGetRequest, PutGetResponse>(16, 512, true);
		NaRPCEndpoint<PutGetRequest, PutGetResponse> endpoint = clientGroup.createEndpoint();
		endpoint.connect(address);	
		
		PutGetRequest request = new PutGetRequest(type, srcFile, dstFile);
		PutGetResponse response = new PutGetResponse((short) 0);
		endpoint.issueRequest(request, response).get();
	}
}
