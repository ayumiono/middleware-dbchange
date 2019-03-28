package com.gb.dblogger.server;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gb.dblogger.remoting.netty.NettyServerConfig;
import com.gb.dblogger.remoting.netty.NettySystemConfig;
import com.gb.dblogger.server.component.ServerController;

/**
 * 服务端启动类
 * 
 * @author xuelong.chen
 *
 */
public class BootStrap {

	private static Logger log = LoggerFactory.getLogger("BootStrap");

	public static Properties properties = null;
	public static CommandLine commandLine = null;
	public static String configFile = null;

	public static void main(String[] args) {
		createController(args).start();
	}

	public static ServerController createController(String[] args) {
		if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
			NettySystemConfig.socketSndbufSize = 131072;
		}

		if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
			NettySystemConfig.socketRcvbufSize = 131072;
		}

		try {
			final NettyServerConfig nettyServerConfig = new NettyServerConfig();

			commandLine = parseCmdLine("dbLoggerServer", args, buildCommandlineOptions(new Options()), new DefaultParser());
			if (commandLine.hasOption("c")) {
				String file = commandLine.getOptionValue("c");
				if (file != null) {
					configFile = file;
					InputStream in = new BufferedInputStream(new FileInputStream(file));
					properties = new Properties();
					properties.load(in);

					properties2Object(properties, nettyServerConfig);
					in.close();
				}
			}

			if (commandLine.hasOption('p')) {
				printObjectProperties(log, nettyServerConfig);
				System.exit(0);
			}

			final ServerController controller = new ServerController(nettyServerConfig);
			if(properties.getProperty("dubboZk") == null) {
				System.out.println("请在配置文件中指定dubbo zk集群地址");
				System.exit(0);
			}
			controller.setDubboZk(properties.getProperty("dubboZk"));
			controller.setNotifyRetryTimes(Integer.parseInt(properties.getProperty("dbchangeEventNotifyRetryTimes","5")));
			
			System.setProperty("secret",properties.getProperty("secret","dblogger-netty-server"));
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				private volatile boolean hasShutdown = false;
				private AtomicInteger shutdownTimes = new AtomicInteger(0);

				@Override
				public void run() {
					synchronized (this) {
						log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
						if (!this.hasShutdown) {
							this.hasShutdown = true;
							long beginTime = System.currentTimeMillis();
							controller.shutdown();
							long consumingTimeTotal = System.currentTimeMillis() - beginTime;
							log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
						}
					}
				}
			}, "ShutdownHook"));
			return controller;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	private static Options buildCommandlineOptions(final Options options) {
		Option opt = new Option("h", "help", false, "Print help");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("c", "configFile", true, "Broker config properties file");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("p", "printConfigItem", false, "Print all config item");
		opt.setRequired(false);
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

	public static void properties2Object(final Properties p, final Object object) {
		Method[] methods = object.getClass().getMethods();
		for (Method method : methods) {
			String mn = method.getName();
			if (mn.startsWith("set")) {
				try {
					String tmp = mn.substring(4);
					String first = mn.substring(3, 4);

					String key = first.toLowerCase() + tmp;
					String property = p.getProperty(key);
					if (property != null) {
						Class<?>[] pt = method.getParameterTypes();
						if (pt != null && pt.length > 0) {
							String cn = pt[0].getSimpleName();
							Object arg = null;
							if (cn.equals("int") || cn.equals("Integer")) {
								arg = Integer.parseInt(property);
							} else if (cn.equals("long") || cn.equals("Long")) {
								arg = Long.parseLong(property);
							} else if (cn.equals("double") || cn.equals("Double")) {
								arg = Double.parseDouble(property);
							} else if (cn.equals("boolean") || cn.equals("Boolean")) {
								arg = Boolean.parseBoolean(property);
							} else if (cn.equals("float") || cn.equals("Float")) {
								arg = Float.parseFloat(property);
							} else if (cn.equals("String")) {
								arg = property;
							} else {
								continue;
							}
							method.invoke(object, arg);
						}
					}
				} catch (Throwable ignored) {
				}
			}
		}
	}

	public static void printObjectProperties(final Logger logger, final Object object) {
		Field[] fields = object.getClass().getDeclaredFields();
		for (Field field : fields) {
			if (!Modifier.isStatic(field.getModifiers())) {
				String name = field.getName();
				if (!name.startsWith("this")) {
					Object value = null;
					try {
						field.setAccessible(true);
						value = field.get(object);
						if (null == value) {
							value = "";
						}
					} catch (IllegalAccessException e) {
						log.error("Failed to obtain object properties", e);
					}

					if (logger != null) {
						logger.info(name + "=" + value);
					} else {
					}
				}
			}
		}
	}
}
