package com.azure.service;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobProperties;

public class MainApplication {

	private static Logger LOGGER = LoggerFactory.getLogger(MainApplication.class);

	// private static StorageQueueCrud storageQueueCrud = new StorageQueueCrud();

	//private static ServiceBusTopicOperations serviceBusTopicOperations = new ServiceBusTopicOperations(
	//		"RootManageSharedAccessKey", System.getenv("AZURE_SERVICE_BUS_KEY"), "az204servicebus2023",
	//		"az204devtopic");


	public static void main(String[] args) throws InterruptedException {
		
	}

	public static void copyFileFromOneContainerToAnother() {

		String acccountConnectionString = System.getenv("AZURE_SERVICE_STORAGE_ACCOUNT_KEY");

		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(acccountConnectionString)
				.buildClient();

		BlobClient sourceBlob = blobServiceClient.getBlobContainerClient("az204-practice-container")
				.getBlobClient("image_1.jpg");
		BlobClient destBlob = blobServiceClient.getBlobContainerClient("az204-practice-container-2")
				.getBlobClient("monkey.jpg");
		final SyncPoller<BlobCopyInfo, Void> poller = destBlob.beginCopy(sourceBlob.getBlobUrl(),
				Duration.ofSeconds(2));

		BlobProperties properties = sourceBlob.getProperties();

		System.out.println(properties.getAccessTier());

	}

}
