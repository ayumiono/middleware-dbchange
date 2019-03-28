package com.gb.dblogger.client.tool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.alibaba.fastjson.JSON;
import com.gb.dblogger.common.protocol.LibUpdEvent;
import com.gb.dblogger.remoting.RemotingClient;
import com.gb.dblogger.remoting.exception.RemotingConnectException;
import com.gb.dblogger.remoting.exception.RemotingSendRequestException;
import com.gb.dblogger.remoting.exception.RemotingTimeoutException;
import com.gb.dblogger.remoting.netty.NettyClientConfig;
import com.gb.dblogger.remoting.netty.NettyRemotingClient;
import com.gb.dblogger.remoting.protocol.DBLoggerServerStatus;
import com.gb.dblogger.remoting.protocol.RemotingCommand;
import com.gb.dblogger.remoting.protocol.RequestCode;
import com.gb.dblogger.remoting.protocol.ResponseCode;
import com.google.common.base.Charsets;

public class DbLoggerCliTool {

	private static CommandLine commandLine = null;

	private static RemotingClient client;
	
	public static void main(String[] args) {
		try {
			commandLine = parseCmdLine("dbLoggerCliCmd", args, buildCommandlineOptions(new Options()), new DefaultParser());
			String serverAddr = commandLine.getOptionValue("server");
			String appid = commandLine.getOptionValue("appid");
			String sign = commandLine.getOptionValue("sign");
			
			if (commandLine.hasOption("s")) {
				String outputFilePath = commandLine.getOptionValue("file");
				client = createRemoteClient();
				snapshot(serverAddr, appid, sign, outputFilePath);
			} else if (commandLine.hasOption("auth")) {
				String ac = commandLine.getOptionValue("auth");
				if (ac == null) {
					System.err.println("please give a token count you want generate");
					return;
				}
				client = createRemoteClient();
				authtoken(serverAddr, appid, sign, Integer.parseInt(ac));
			} else if(commandLine.hasOption("luc")) {
				client = createRemoteClient();
				libUpdCache(serverAddr, appid, sign);
			} else if(commandLine.hasOption("cluc")) {
				String projectName = commandLine.getOptionValue("auth");
				if(projectName == null) {
					System.err.println("please give a projectName that you what clear, or * to clear all cache");
					return;
				}
				client = createRemoteClient();
				clearLibUpdCache(serverAddr, appid, sign, projectName);
			} else if(commandLine.hasOption("jcs")) {
				String outputFilePath = commandLine.getOptionValue("file");
				client = createRemoteClient();
				jettyContainerSnapshot(serverAddr, appid, sign, outputFilePath);
			}
		} finally {
			if(client != null) {
				client.shutdown();
			}
		}
	}
	
	private static void clearLibUpdCache(String serveraddr, String appid, String sign, String projectName) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAR_LIB_UPD_CACHE, null);
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", appid);
		extfield.put("sign", sign);
		request.setExtFields(extfield);
		try {
			request.setBody(projectName.getBytes("utf-8"));
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
			return;
		}
		try {
			RemotingCommand response = client.invokeSync(serveraddr, request, 3000);
			System.out.println(response.getRemark());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void libUpdCache(String serveraddr, String appid, String sign) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LIB_UPD_CACHE, null);
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", appid);
		extfield.put("sign", sign);
		request.setExtFields(extfield);
		try {
			RemotingCommand response = client.invokeSync(serveraddr, request, 3000);
			if(response.getCode() != ResponseCode.SUCCESS) {
				System.out.println(response.getRemark());
				return;
			}
			List<LibUpdEvent> libUpdEventCache = JSON.parseArray(new String(request.getBody()), LibUpdEvent.class);
			libUpdEventCache.forEach(new Consumer<LibUpdEvent>() {
				@Override
				public void accept(LibUpdEvent t) {
					StringBuilder sbuilder = new StringBuilder("LibUpdEvent[");
					sbuilder.append("projectName:"+t.getProjectName());
					sbuilder.append(", fileName:"+t.getFileName());
					sbuilder.append(", name:"+t.getName());
					sbuilder.append(", groupName:"+t.getGroupName());
					sbuilder.append(", version:" + t.getVersion());
					sbuilder.append("]");
					System.out.println(sbuilder.toString());
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void jettyContainerSnapshot(String serveraddr, String appid, String sign, String filepath) {
		
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.JETTY_CONTAINER_STATUS, null);
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", appid);
		extfield.put("sign", sign);
		request.setExtFields(extfield);
		try {
			RemotingCommand response = client.invokeSync(serveraddr, request, 3000);
			
			if(response.getCode() != ResponseCode.SUCCESS) {
				System.out.println(response.getRemark());
				return;
			}
			
			DBLoggerServerStatus status = DBLoggerServerStatus.decode(response.getBody(), DBLoggerServerStatus.class);

			if(filepath == null) {
				System.out.println(String.format("当前存活的客户端有：%d 个。各客户端容器运行情况如下：", status.getSessions()));
				for (Entry<String, Object> v : status.getVersions().entrySet()) {
					System.out.println(String.format("%s \t %s", v.getKey(), String.valueOf(v.getValue())));
				}
			}else {
				File file = new File(filepath);
				if (!file.exists()) {
					file.getParentFile().mkdir();
					try {
				        file.createNewFile();
				        file.setWritable(true);
				    } catch (IOException e) {
				        e.printStackTrace();
				        return;
				    }
				}
				FileWriter fileWriter = new FileWriter(file, true);
				fileWriter.write(String.format("当前存活的客户端有：%d 个。各客户端容器运行情况如下："+System.lineSeparator(), status.getSessions()));
				for (Entry<String, Object> v : status.getVersions().entrySet()) {
					fileWriter.write(String.format("%s \t %s"+System.lineSeparator(), v.getKey(), String.valueOf(v.getValue())));
				}
				fileWriter.flush();
				fileWriter.close();
			}
			
		} catch (RemotingConnectException | RemotingSendRequestException | RemotingTimeoutException
				| InterruptedException | IOException e) {
			e.printStackTrace();
			return;
		}
	}

	private static void snapshot(String serveraddr, String appid, String sign, String filepath) {
		
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DBLOGGER_SERVER_STATUS, null);
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", appid);
		extfield.put("sign", sign);
		request.setExtFields(extfield);
		try {
			RemotingCommand response = client.invokeSync(serveraddr, request, 3000);
			
			if(response.getCode() != ResponseCode.SUCCESS) {
				System.out.println(response.getRemark());
				return;
			}
			
			DBLoggerServerStatus status = DBLoggerServerStatus.decode(response.getBody(), DBLoggerServerStatus.class);

			if(filepath == null) {
				System.out.println(String.format("当前存活的客户端有：%d 个。各客户端数据版本如下：", status.getSessions()));
				for (Entry<String, Object> v : status.getVersions().entrySet()) {
					System.out.println(String.format("%s \t %s", v.getKey(), String.valueOf(v.getValue())));
				}
			}else {
				File file = new File(filepath);
				if (!file.exists()) {
					file.getParentFile().mkdir();
					try {
				        file.createNewFile();
				        file.setWritable(true);
				    } catch (IOException e) {
				        e.printStackTrace();
				        return;
				    }
				}
				FileWriter fileWriter = new FileWriter(file, true);
				fileWriter.write(String.format("当前存活的客户端有：%d 个。各客户端数据版本如下："+System.lineSeparator(), status.getSessions()));
				for (Entry<String, Object> v : status.getVersions().entrySet()) {
					fileWriter.write(String.format("%s \t %s"+System.lineSeparator(), v.getKey(), String.valueOf(v.getValue())));
				}
				fileWriter.flush();
				fileWriter.close();
			}
			
		} catch (RemotingConnectException | RemotingSendRequestException | RemotingTimeoutException
				| InterruptedException | IOException e) {
			e.printStackTrace();
			return;
		}
	}

	@SuppressWarnings({ "rawtypes" })
	private static void authtoken(String serveraddr, String appid, String sign, int countneed) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GEN_AUTH, null);
		HashMap<String, String> extfield = new HashMap<>();
		extfield.put("appId", appid);
		extfield.put("sign", sign);
		extfield.put("count", countneed+"");
		request.setExtFields(extfield);
		try {
			RemotingCommand response = client.invokeSync(serveraddr, request, 3000);
			if(response.getCode() != ResponseCode.SUCCESS) {
				System.out.println(response.getRemark());
				return;
			}
			List tokens = JSON.parseArray(new String(response.getBody(), Charsets.UTF_8), List.class);
			Iterator iterator = tokens.iterator();
			while (iterator.hasNext()) {
				System.out.println(iterator.next());
			}
		} catch (RemotingConnectException | RemotingSendRequestException | RemotingTimeoutException
				| InterruptedException e) {
			e.printStackTrace();
			return;
		}
	}

	private static RemotingClient createRemoteClient() {
		RemotingClient client = new NettyRemotingClient(new NettyClientConfig());
		client.start();
		return client;
	}

	private static Options buildCommandlineOptions(final Options options) {
		Option opt = new Option("h", "help", false, "Print help");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("auth", "auth", true, "generate auth token, pass tokens count you want make");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);

		opt = new Option("s", "snapshot", false, "get dblogger server status snapshot");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("server", "server", true, "dblogger server address");
		opt.setRequired(true);
		opt.setArgs(1);
		options.addOption(opt);

		opt = new Option("appid", "appid", true, "auth required appid,get it from the SP");
		opt.setRequired(true);
		opt.setArgs(1);
		options.addOption(opt);

		opt = new Option("sign", "sign", true, "auth required sign,get it from the SP");
		opt.setRequired(true);
		opt.setArgs(1);
		options.addOption(opt);

		opt = new Option("file", "outputfile", true, "output file");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);
		
		opt = new Option("luc", "luc", false, "libupdevent cache");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);
		
		opt = new Option("cluc", "cluc", true, "clear libupdevent cache");
		opt.setRequired(false);
		opt.setArgs(1);
		options.addOption(opt);
		
		opt = new Option("jcs", "jcs", false, "jetty container status");
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
