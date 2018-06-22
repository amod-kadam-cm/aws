package com.cloudmanthan.training.aws.s3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.cloudmanthan.training.aws.SetupTest;

public class S3Demo {
	static final Logger logger = Logger.getLogger(S3Demo.class);
	// Create a Service Client using ServiceClient Builder
	static AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
	
	public static void main(String[] args) throws Exception {
		//listBuckets();
		//createBucket();
		// Working with Objects 
		//uploadFile();
		// 
		//listObjects(); 
		//downloadObjec();
		//trasnferManagerDemo();
		//transferManagerBlockingDemo();
		
		//transferManagerDemoProgressUsingLister();
		downloadDemo();
		
		
		
	}
	
	private static void transferManagerBlockingDemo() throws InterruptedException {
		//String file_path = "D:\\Softwares\\LoginApp.war";
		
		
		String file_path = "D:\\Softwares\\eclipse-jee-neon-2-win32-x86_64.zip"; //(308 MB)
		String bucket_name = "amod.kadam";
		
		File f = new File(file_path);
		TransferManager xfer_mgr = new TransferManager();
		try {
		    String key_name = file_path;
			Upload xfer = xfer_mgr.upload(bucket_name, key_name , f);
			
		    // loop with Transfer.isDone()
			do {
				
				logger.info("wating till transfer is compelte");
				logger.info("Total Bytes to transfer" + xfer.getProgress().getTotalBytesToTransfer());
				logger.info("Bytes transferred" + xfer.getProgress().getBytesTransfered());
				logger.info("%ge transferred " + xfer.getProgress().getPercentTransferred());
				
			} while (xfer.isDone());
		    //  or block with Transfer.waitForCompletion()
			
			logger.warn("File uploaded");
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		xfer_mgr.shutdownNow();
				
	}
	
	
	private static void downloadDemo(){
		String bucketName = "amod.kadam";
		String key = "s3/IntelliJIDE-Installable-new";
		
		File downloadeToFile = new File("d://downloaded_using_xfer_manager");
		
		TransferManager xfer_mgr = new TransferManager();
		
		Download download = xfer_mgr.download(bucketName, key,downloadeToFile);
		
		try {
			download.waitForCompletion();
		} catch (AmazonServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AmazonClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		xfer_mgr.shutdownNow();
		
		
		
		
		
	}
	
	@SuppressWarnings("deprecation")
	private static void transferManagerDemoProgressUsingLister() throws InterruptedException {
//		String file_path = "D:\\Softwares\\LoginApp.war";
		String file_path = "D:\\Softwares\\eclipse-jee-neon-2-win32-x86_64.zip"; //(308 MB)
		String bucket_name = "amod.kadam";
		
		File f = new File(file_path);
		TransferManager xfer_mgr = new TransferManager();
		try {
		    String key_name = file_path;
			Upload xfer = xfer_mgr.upload(bucket_name, key_name , f);
			
					
			xfer.addProgressListener(new ProgressListener(){
				
				@Override
				public void progressChanged(ProgressEvent e) {
					   double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
					   logger.info("pct uploaded" + pct);
					   
					   
				}
			});
			
			xfer.waitForCompletion();
			TransferState xfer_state = xfer.getState();
		    System.out.println(": " + xfer_state);
				
			logger.warn("File uploaded");
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		xfer_mgr.shutdownNow();
	}

	private static void transferManagerDemoProgress() throws InterruptedException {

		
		String file_path = "D:\\Softwares\\LoginApp.war";
		String bucket_name = "amod.kadam";
		
		File f = new File(file_path);
		TransferManager xfer_mgr = new TransferManager();
		try {
		    String key_name = "Uploaded_using_tx_mgr_LoginApp.war";
			Upload xfer = xfer_mgr.upload(bucket_name, key_name , f);
			
		    // loop with Transfer.isDone()
			do {
				
				
				logger.info("wating till transfer is compelte");
				logger.info("Total Bytes to transfer" + xfer.getProgress().getTotalBytesToTransfer());
				logger.info("Bytes transferred" + xfer.getProgress().getBytesTransfered());
				logger.info("%ge transferred " + xfer.getProgress().getPercentTransferred());
				
			} while (xfer.isDone() == false );
		    //  or block with Transfer.waitForCompletion()
			
			logger.warn("File uploaded");
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		xfer_mgr.shutdownNow();

		// do the same with progress lister
		// poll the status 

	}
	private static void downloadObjec() {
		String bucket_name = "amod.kadam";
		String key_name = "amodemoapp.zip";
		try {
		    S3Object o = s3Client.getObject(bucket_name, key_name);
		    S3ObjectInputStream s3is = o.getObjectContent();
		    FileOutputStream fos = new FileOutputStream(new File(key_name));
		    byte[] read_buf = new byte[1024];
		    int read_len = 0;
		    while ((read_len = s3is.read(read_buf)) > 0) {
		        fos.write(read_buf, 0, read_len);
		    }
		    s3is.close();
		    fos.close();
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		} catch (FileNotFoundException e) {
		    System.err.println(e.getMessage());
		    System.exit(1);
		} catch (IOException e) {
		    System.err.println(e.getMessage());
		    System.exit(1);
		}

		
	}

	private static void listObjects() {
		String bucket_name = "amod.kadam";
		ObjectListing ol = s3Client.listObjects(bucket_name);
		List<S3ObjectSummary> objects = ol.getObjectSummaries();
		for (S3ObjectSummary os: objects) {
		    System.out.println("* " + os.getKey());
		}


		
	}

	private static void uploadFile() {
		String bucket_name = "amod.kadam";
		String key_name = "uploaded_using_sdk";
		String  file_path = "D:\\Training\\EC2Instance.rar";
		try {
		    s3Client.putObject(bucket_name, key_name, file_path);
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		
	}

	static void createBucket(){
		
		try { 
		String bucketName = "amod.kadam.sdk" + UUID.randomUUID();
		
		// use the client to create bucket
		s3Client.createBucket(bucketName);
		logger.info("Bucket is created with name" + bucketName );
		} catch (AmazonServiceException awsEx){
			
			awsEx.printStackTrace();
		}
		

	}
		
	
	
	static void listBuckets(){
		
		List<Bucket> bucketList =  s3Client.listBuckets();
		
		Iterator<Bucket> bucketIterator = bucketList.iterator();
		
		while (bucketIterator.hasNext()) {
			logger.info("Bucket is" + bucketIterator.next());
			
		}
		
	}
}
