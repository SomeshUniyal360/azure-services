package com.azure.service.crud;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusTransactionContext;
import com.azure.messaging.servicebus.models.CompleteOptions;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.service.util.AzureUtil;

import reactor.core.Disposable;

public class ServiceBusTopicOperations {

	private static Logger LOGGER = LoggerFactory.getLogger(ServiceBusTopicOperations.class);
	private ServiceBusClientBuilder serviceBusClientBuilder;
	private String topicName;

	/*
	 * construct ServiceBusTopicOperations object
	 */
	public ServiceBusTopicOperations(String keyName, String key, String nameSpace, String topicName) {

		String sharedAccesskeyName = keyName;
		String sharedAccesskey = key;
		String uri = String.format("%s.servicebus.windows.net/%s", nameSpace, topicName);

		String sharedAccessSignature = AzureUtil.getSASToken(uri, sharedAccesskeyName, sharedAccesskey);

		String connectionString = String.format("Endpoint=%s.servicebus.windows.net;SharedAccessSignature=%s",
				nameSpace, sharedAccessSignature);
		System.out.println("SAS Token:  " + connectionString);
		this.serviceBusClientBuilder = new ServiceBusClientBuilder().connectionString(connectionString);
		this.topicName = topicName;
	}

	/*
	 * This method will send multiple messages to services bus topic using Sync
	 * client
	 * 
	 * @param msgList
	 * 
	 * @param topicName
	 * 
	 * @return
	 */
	public void sendSyncMessagesToServiceBusTopic(List<String> msgList) {
		ServiceBusSenderClient messagesSyncSender = serviceBusClientBuilder.sender().topicName(topicName).buildClient();
		List<ServiceBusMessage> messages = new ArrayList<ServiceBusMessage>();
		ServiceBusTransactionContext transaction = messagesSyncSender.createTransaction();
		try {
			for (String message : msgList) {
				ServiceBusMessage busMsg = new ServiceBusMessage(message);
				messages.add(busMsg);
			}
			messagesSyncSender.sendMessages(messages, transaction);
			messagesSyncSender.commitTransaction(transaction);
			LOGGER.info(String.format("Successfully send %d messages to topic %s", msgList.size(), topicName));
		} catch (Exception e) {
			LOGGER.error("Exception occured while sending messages to service bus", e);
			messagesSyncSender.rollbackTransaction(transaction);
		} finally {
			messagesSyncSender.close();
		}
	}

	/*
	 * This method will receive multiple messages from services bus topic using Sync
	 * receiver client Note: If receiver has received less number of messages then
	 * it will wait for timeout time
	 * 
	 * @param topicName
	 * 
	 * @param subscriberName
	 * 
	 * @return msgList
	 */
	public List<String> receiveSyncMessageFromServiceBusTopicUsingReceiver(String subscriberName) {
		List<String> msgList = new ArrayList<String>();
		ServiceBusReceiverClient receiverSyncClient = serviceBusClientBuilder.receiver().topicName(topicName)
				.disableAutoComplete().subscriptionName(subscriberName).receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
				.buildClient();
		ServiceBusTransactionContext transaction = receiverSyncClient.createTransaction();
		try {
			IterableStream<ServiceBusReceivedMessage> iterableStream = receiverSyncClient.receiveMessages(100,
					Duration.ofSeconds(30));
			iterableStream.forEach((receivedMsg) -> {
				msgList.add(receivedMsg.getBody().toString());
				receiverSyncClient.complete(receivedMsg, new CompleteOptions().setTransactionContext(transaction));
			});
			receiverSyncClient.commitTransaction(transaction);

			LOGGER.info("receiveSyncMessageFromServiceBusTopicUsingReceiver Operation completed");
			LOGGER.info(String.format("receiveSyncMessageFromServiceBusTopicUsingReceiver received %d messages",
					msgList.size()));
		} catch (Exception e) {
			LOGGER.error("Exception occured while receiving messages from service bus", e);
			receiverSyncClient.rollbackTransaction(transaction);
		} finally {
			receiverSyncClient.close();
		}
		return msgList;
	}

	/*
	 * This method will send multiple messages to services bus topic using Async
	 * client
	 * 
	 * @param msgList
	 * 
	 * @param topicName
	 * 
	 * @return
	 */
	public void sendAsyncMessagesToServiceBusTopic(List<String> msgList) throws InterruptedException {
		ServiceBusSenderAsyncClient messagesAsyncSender = serviceBusClientBuilder.sender().topicName(topicName)
				.buildAsyncClient();
		Disposable disposable = null;
		List<ServiceBusMessage> messages = new ArrayList<ServiceBusMessage>();
		for (String message : msgList) {
			ServiceBusMessage busMsg = new ServiceBusMessage(message);
			messages.add(busMsg);
		}
		try {
			disposable = messagesAsyncSender.sendMessages(messages).doOnSuccess((x) -> {
				LOGGER.info(String.format("Successfully send %d messages to topic %s", msgList.size(), topicName));
			}).doOnError((error) -> {
				LOGGER.error("sendAsyncMessagesToServiceBusTopic failed");
			}).doFinally((signal) -> {
				LOGGER.info("sendAsyncMessagesToServiceBusTopic signal from finally" + signal);
				messagesAsyncSender.close();
			}).subscribe((x) -> {
			});
			Thread.sleep(30000);// this sleep should not be used with async client
		} catch (Exception e) {
			LOGGER.error("Exception occured while sending async messages from service bus", e);
		} finally {
			if (disposable != null) {
				disposable.dispose();
			}
			messagesAsyncSender.close();
		}
	}

	/*
	 * This method will receive multiple messages from services bus topic using
	 * Async receiver client Note: If receiver has received less number of messages
	 * then it will wait for timeout time
	 * 
	 * @param topicName
	 * 
	 * @param subscriberName
	 * 
	 * @return msgList
	 */
	public List<String> receiveAsyncMessageFromServiceBusTopicUsingReceiver(String subscriberName)
			throws InterruptedException {
		List<String> msgList = new ArrayList<String>();

		ServiceBusReceiverAsyncClient receiverAsyncClient = serviceBusClientBuilder.receiver().topicName(topicName)
				.subscriptionName(subscriberName).disableAutoComplete().receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
				.buildAsyncClient();
		Disposable disposable = null;
		try {
			disposable = receiverAsyncClient.receiveMessages().flatMap(messageReceived -> {
				msgList.add(messageReceived.getBody().toString());
				return receiverAsyncClient.complete(messageReceived);
			}).subscribe((x) -> {

			});

			Thread.sleep(30000);// this sleep should not be used with async client
			LOGGER.info("receiveAsyncMessageFromServiceBusTopicUsingReceiver Operation completed");
			LOGGER.info(String.format("receiveAsyncMessageFromServiceBusTopicUsingReceiver received %d messages",
					msgList.size()));
		} catch (Exception e) {
			LOGGER.error("Exception occured while receiving async messages from service bus", e);
		} finally {
			if (disposable != null) {
				disposable.dispose();
			}
			receiverAsyncClient.close();
		}

		return msgList;
	}

}
