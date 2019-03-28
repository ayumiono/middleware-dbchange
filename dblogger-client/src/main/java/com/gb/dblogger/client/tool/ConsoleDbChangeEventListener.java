package com.gb.dblogger.client.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.gb.dblogger.client.DbLoggerClient;
import com.gb.dblogger.client.component.DbChangeEventListener;
import com.gb.dblogger.common.protocol.DbChangeEvent;
import com.gb.dblogger.remoting.exception.DBChangeEventException;

public class ConsoleDbChangeEventListener {
	
	public static CommandLine commandLine = null;
	
	public static void main(String[] args) {
		commandLine = parseCmdLine("dbLogger", args, buildCommandlineOptions(new Options()), new DefaultParser());
		String serverAddr = commandLine.getOptionValue("server");
		String appid = commandLine.getOptionValue("appid");
		String sign = commandLine.getOptionValue("sign");
		Integer heartBeatInterval = Integer.parseInt(commandLine.getOptionValue("heartbeat"));
		DbLoggerClient consoleClient = new DbLoggerClient();
		consoleClient.setAddr(serverAddr);
		consoleClient.setAppId(appid);
		consoleClient.setHeartBeatInterval(heartBeatInterval);
		consoleClient.setSign(sign);
		consoleClient.registerDbChangeEventProcessor(new DbChangeEventListener() {
			@Override
			public long deal(DbChangeEvent dbLog) throws DBChangeEventException {
				System.out.println(dbLog.toString());
				return 0;
			}

			@Override
			public boolean alterTabel(String sql) {
				System.out.println(sql);
				return true;
			}
		});
		consoleClient.start();
	}
	
	
	private static Options buildCommandlineOptions(final Options options) {
		Option opt = new Option("h", "help", false, "Print help");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("server", "server", true, "dblogger server address");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);

		opt = new Option("heartbeat", "heartbeat", false, "heartbeat time interval");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);
		
		opt = new Option("appid", "appid", false, "auth required appid,get it from the SP");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);
		
		opt = new Option("sign", "sign", false, "auth required sign,get it from the SP");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);

		return options;
	}
	
	private static CommandLine parseCmdLine(final String appName, String[] args, Options options,
			CommandLineParser parser) {
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(110);
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption('h')) {
				hf.printHelp(appName, options, true);
				return null;
			}
		} catch (ParseException e) {
			hf.printHelp(appName, options, true);
		}

		return commandLine;
	}
}
