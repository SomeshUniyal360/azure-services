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

public class ServiceBusQueueOperations {

	private static Logger LOGGER = LoggerFactory.getLogger(ServiceBusQueueOperations.class);
	private ServiceBusClientBuilder serviceBusClientBuilder;
	private String queueName;

	/*
	 * construct ServiceBusQueueOperations object
	 */
	public ServiceBusQueueOperations(String keyName, String key, String nameSpace, String queueName) {

		String sharedAccesskeyName = keyName;
		String sharedAccesskey = key;
		String uri = String.format("%s.servicebus.windows.net/%s", nameSpace, queueName);

		String sharedAccessSignature = AzureUtil.getSASToken(uri, sharedAccesskeyName, sharedAccesskey);

		String connectionString = String.format("Endpoint=%s.servicebus.windows.net;SharedAccessSignature=%s",
				nameSpace, sharedAccessSignature);
		this.serviceBusClientBuilder = new ServiceBusClientBuilder().connectionString(connectionString);
		this.queueName = queueName;
	}

	/*
	 * This method will send multiple messages to services bus queue using Sync
	 * client
	 * 
	 * @param msgList
	 * 
	 * @param queueName
	 * 
	 * @return
	 */
	public void sendSyncMessagesToServiceBusQueue(List<String> msgList) {
		ServiceBusSenderClient messagesSyncSender = serviceBusClientBuilder.sender().queueName(queueName).buildClient();
		List<ServiceBusMessage> messages = new ArrayList<ServiceBusMessage>();
		ServiceBusTransactionContext transaction = messagesSyncSender.createTransaction();
		try {
			for (String message : msgList) {
				ServiceBusMessage busMsg = new ServiceBusMessage(message);
				messages.add(busMsg);
			}
			messagesSyncSender.sendMessages(messages, transaction);
			messagesSyncSender.commitTransaction(transaction);
			LOGGER.info(String.format("Successfully send %d messages to queue %s", msgList.size(), queueName));
		} catch (Exception e) {
			LOGGER.error("Exception occured while sending messages to service bus", e);
			messagesSyncSender.rollbackTransaction(transaction);
		} finally {
			messagesSyncSender.close();
		}
	}

	/*
	 * This method will receive multiple messages from services bus queue using Sync
	 * receiver client
	 * 
	 * @param queueName
	 * 
	 * @param subscriberName
	 * 
	 * @return msgList
	 */
	public List<String> receiveSyncMessageFromServiceBusQueueUsingReceiver() {
		List<String> msgList = new ArrayList<String>();
		ServiceBusReceiverClient receiverSyncClient = serviceBusClientBuilder.receiver().queueName(queueName)
				.disableAutoComplete().receiveMode(ServiceBusReceiveMode.PEEK_LOCK).buildClient();
		ServiceBusTransactionContext transaction = receiverSyncClient.createTransaction();
		try {
			IterableStream<ServiceBusReceivedMessage> iterableStream = receiverSyncClient.receiveMessages(100,
					Duration.ofSeconds(30));
			iterableStream.forEach((receivedMsg) -> {
				msgList.add(receivedMsg.getBody().toString());
				receiverSyncClient.complete(receivedMsg, new CompleteOptions().setTransactionContext(transaction));
			});
			receiverSyncClient.commitTransaction(transaction);

			LOGGER.info("receiveSyncMessageFromServiceBusQueueUsingReceiver Operation completed");
			LOGGER.info(String.format("receiveSyncMessageFromServiceBusQueueUsingReceiver received %d messages",
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
	 * This method will send multiple messages to services bus queue using Async
	 * client
	 * 
	 * @param msgList
	 * 
	 * @param queueName
	 * 
	 * @return
	 */
	public void sendAsyncMessagesToServiceBusQueue(List<String> msgList) throws InterruptedException {
		ServiceBusSenderAsyncClient messagesAsyncSender = serviceBusClientBuilder.sender().queueName(queueName)
				.buildAsyncClient();
		Disposable disposable = null;
		List<ServiceBusMessage> messages = new ArrayList<ServiceBusMessage>();
		for (String message : msgList) {
			ServiceBusMessage busMsg = new ServiceBusMessage(message);
			messages.add(busMsg);
		}
		try {
			disposable = messagesAsyncSender.sendMessages(messages).doOnSuccess((x) -> {
				LOGGER.info(String.format("Successfully send %d messages to queue %s", msgList.size(), queueName));
			}).doOnError((error) -> {
				LOGGER.error("sendAsyncMessagesToServiceBusQueue failed");
			}).doFinally((signal) -> {
				LOGGER.info("sendAsyncMessagesToServiceBusQueue signal from finally " + signal);
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
	 * This method will receive multiple messages from services bus queue using
	 * Async receiver client
	 * 
	 * @param queueName
	 * 
	 * @param subscriberName
	 * 
	 * @return msgList
	 */
	public List<String> receiveAsyncMessageFromServiceBusQueueUsingReceiver() throws InterruptedException {
		List<String> msgList = new ArrayList<String>();

		ServiceBusReceiverAsyncClient receiverAsyncClient = serviceBusClientBuilder.receiver().queueName(queueName)
				.disableAutoComplete().receiveMode(ServiceBusReceiveMode.PEEK_LOCK).buildAsyncClient();
		Disposable disposable = null;
		try {
			disposable = receiverAsyncClient.receiveMessages().flatMap(messageReceived -> {
				msgList.add(messageReceived.getBody().toString());
				return receiverAsyncClient.complete(messageReceived);
			}).subscribe((x) -> {

			});

			Thread.sleep(30000);// this sleep should not be used with async client
			LOGGER.info("receiveAsyncMessageFromServiceBusQueueUsingReceiver Operation completed");
			LOGGER.info(String.format("receiveAsyncMessageFromServiceBusQueueUsingReceiver received %d messages",
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
