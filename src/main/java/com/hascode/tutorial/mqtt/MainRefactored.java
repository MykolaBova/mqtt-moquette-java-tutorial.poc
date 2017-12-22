package com.hascode.tutorial.mqtt;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathResourceLoader;
import io.moquette.server.config.IConfig;

import io.moquette.server.config.IResourceLoader;
import io.moquette.server.config.ResourceLoaderConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MainRefactored {

	public Server mqttBroker;

	public static void main(String[] args) throws InterruptedException, IOException {
		MainRefactored mainRefactored = new MainRefactored();
		mainRefactored.mqttBroker = new Server();

		mainRefactored.initMWTTServer();

		mainRefactored.publishWithMQTTClient();

		mainRefactored.stopMQTTServer();

		mainRefactored.cleanupDbFiles();

		System.exit(0);
	}

	private void stopMQTTServer() {
		if(this.mqttBroker != null) {
			System.out.println("stopping serer");
			this.mqttBroker.stopServer();
			System.out.println("serer is stopped");
		}
	}

	private void cleanupDbFiles() {
		String DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME = "moquette_store.mapdb";
		String DEFAULT_PERSISTENT_PATH = System.getProperty("user.dir") + File.separator
				+ DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
		System.out.println("----> DB file location: " + DEFAULT_PERSISTENT_PATH);

		String fileName = DEFAULT_PERSISTENT_PATH;
		File dbFile = new File(fileName);
		if (dbFile.exists()) {
			dbFile.delete();
			new File(fileName + ".p").delete();
			new File(fileName + ".t").delete();
		}
		assert(!dbFile.exists());
	}

	private void publishWithMQTTClient() {
		String topic = "/journey/b";
		String content = "Visit www.hascode.com! :D";
		int qos = 2;
		String broker = "tcp://0.0.0.0:1883";
		String clientId = "paho-java-client";

		try {
			MqttClient sampleClient = new MqttClient(broker, clientId, new MemoryPersistence());
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			System.out.println("paho-client connecting to broker: " + broker);
			sampleClient.connect(connOpts);
			System.out.println("paho-client connected to broker");
			System.out.println("paho-client publishing message: " + content);
			MqttMessage message = new MqttMessage(content.getBytes());
			message.setRetained(true);
			message.setQos(qos);
			sampleClient.publish(topic, message);
			System.out.println("paho-client message published");
			sampleClient.disconnect();
			System.out.println("paho-client disconnected");
		} catch (MqttException me) {
			me.printStackTrace();
		}
	}

	private void initMWTTServer() throws IOException, InterruptedException {
		IResourceLoader classpathLoader = new ClasspathResourceLoader();
		final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);

		final List<? extends InterceptHandler> userHandlers = Arrays.asList(new PublisherListener());
		mqttBroker.startServer(classPathConfig, userHandlers);

		///addShutDownHook();

		Thread.sleep(4000);
	}

	private void addShutDownHook() {
		System.out.println("moquette mqtt broker started, press ctrl-c to shutdown..");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("stopping moquette mqtt broker..");
				mqttBroker.stopServer();
				System.out.println("moquette mqtt broker stopped");

				System.exit(0);
			}
		});
	}

	static class PublisherListener extends AbstractInterceptHandler {
		@Override
		public void onPublish(InterceptPublishMessage message) {
			System.out.println("moquette mqtt broker message intercepted, topic: " + message.getTopicName()
					+ ", content: " + new String(message.getPayload().array()));
		}
	}
}