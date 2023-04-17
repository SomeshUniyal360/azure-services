package com.azure.service.crud;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.http.rest.Response;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueProperties;
import com.azure.storage.queue.models.SendMessageResult;

public class StorageQueueCrud {

	private String storageAccountConnectionString = System.getenv("AZURE_SERVICE_STORAGE_ACCOUNT_KEY");
	private QueueServiceClient queueServiceClient;
	private static Logger LOGGER = LoggerFactory.getLogger(StorageQueueCrud.class);

	/*
	 * construct StorageQueueCrud object
	 */
	public StorageQueueCrud() {
		this.queueServiceClient = new QueueServiceClientBuilder().connectionString(storageAccountConnectionString)
				.buildClient();

	}

	/*
	 * This method send message to the storage queue
	 * 
	 * @param message
	 * 
	 * @param queueName
	 * 
	 * @return
	 */
	public void sendMessagesToQueue(String message, String queueName) {
		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		Response<SendMessageResult> resp = queueClient.sendMessageWithResponse(message, null, null, null, null);
		SendMessageResult enqueuedMsg = resp.getValue();
		LOGGER.info("Successfully send message to storage queue:" + queueName);
		LOGGER.info("New MessageId:" + enqueuedMsg.getMessageId());
		LOGGER.info("New Pop Receipt Id:" + enqueuedMsg.getPopReceipt());
	}

	/*
	 * This method read the messages from storage queue and return them in list
	 * 
	 * @param queueName
	 * 
	 * @param count
	 * 
	 * @return message list
	 */
	public List<String> readMessagesFromQueue(String queueName, int count) {
		List<String> messageList = new ArrayList<String>();
		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		Iterator<QueueMessageItem> msgIterator = queueClient
				.receiveMessages(count, Duration.ofSeconds(30), Duration.ofSeconds(50), null).iterator();
		while (msgIterator.hasNext()) {
			QueueMessageItem msgItem = msgIterator.next();
			messageList.add(msgItem.getBody().toString());
			queueClient.deleteMessage(msgItem.getMessageId(), msgItem.getPopReceipt());

		}
		LOGGER.info("Messages retrived from queue: " + messageList);
		return messageList;
	}

	/*
	 * This method read single message from storage queue
	 * 
	 * @param queueName
	 * 
	 * @return message
	 */
	public QueueMessageItem readSingleMessageFromQueue(String queueName) {
		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		QueueMessageItem msgItem = queueClient.receiveMessage();
		LOGGER.info("Message retrived from queue: " + msgItem.getBody().toString());
		return msgItem;
	}

	/*
	 * This method return approximate number of messages in storage queue
	 * 
	 * @param queueName
	 * 
	 * @return message count
	 */
	public int approxMessageCount(String queueName) {
		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		QueueProperties properties = queueClient.getProperties();
		int count = properties.getApproximateMessagesCount();
		LOGGER.info(String.format("Number of messages in queue \"%s\" : %d", queueName, count));
		return count;

	}

	/*
	 * This method will delete a particular message from storage queue
	 * 
	 * @param msgItem
	 * 
	 * @param queueName
	 * 
	 * @return
	 */
	public void deleteMessagesFromQueue(QueueMessageItem msgItem, String queueName) {

		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		LOGGER.info("Message deleted from queue: " + msgItem.getBody().toString());
		LOGGER.info("Message Id of deleted Msg: " + msgItem.getMessageId());
		LOGGER.info("Pop Receipt of deleted Msg: " + msgItem.getPopReceipt());
		queueClient.deleteMessage(msgItem.getMessageId(), msgItem.getPopReceipt());

	}

	/*
	 * This method will update a particular message in storage queue
	 * 
	 * @param msgItem
	 * 
	 * @param queueName
	 * 
	 * @param newMsg
	 * 
	 * @return
	 */
	public void updateMessagesInQueue(QueueMessageItem msgItem, String queueName, String newMsg) {

		QueueClient queueClient = this.queueServiceClient.getQueueClient(queueName);
		queueClient.updateMessage(msgItem.getMessageId(), msgItem.getPopReceipt(), newMsg, null);

		LOGGER.info("Successfully updated message in storage queue");
		LOGGER.info("Message Id of updated msg:" + msgItem.getMessageId());

	}
}
