package com.cloudmantahan.aws.kinesis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.Record;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Shard;

public class GetRecords {

	
	public static final String  myStreamName = "amodjd21061";
	
	 // Set to static as there is one logger per class.
	private static final Logger logger = Logger.getLogger(GetRecords.class.getName());
	public static void main(String[] args) {
		
		
		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.defaultClient();
		
		DescribeStreamRequest describeStreamRequest  = new DescribeStreamRequest();
		describeStreamRequest.setStreamName( myStreamName );
		List<Shard> shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
		    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
		    DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
		    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
		    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
		        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
		    } else {
		        exclusiveStartShardId = null;
		    }
		} while ( exclusiveStartShardId != null );
		
        System.out.println("No. of shards are " + shards.size() + shards.toString());
		
             
		// Get the shard iterator
		String shardIterator;
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
	
		//Shard shard = shards.get(index);
		
		getShardIteratorRequest.setStreamName(myStreamName);
		getShardIteratorRequest.setShardId( "shardId-000000000003"/*,shard.getShardId()*/);
		getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();
		
		// Get Record from shard Iterator
		
		
		
		// Continuously read data records from a shard
		List<com.amazonaws.services.kinesis.model.Record> records;
		    
		while ((true) && (shardIterator != null )) {
		   
		  // Create a new getRecordsRequest with an existing shardIterator 
		  // Set the maximum records to return to 25
		  GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		  getRecordsRequest.setShardIterator(shardIterator);
		  getRecordsRequest.setLimit(25); 
		  GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);
		  // Put the result into record list. The result can be empty.
		  records = result.getRecords();
		  
		  System.out.println(records);
		  
		  System.out.println(records);
		  
		  Iterator<com.amazonaws.services.kinesis.model.Record> recordIterator = records.iterator();
		  
		  while (recordIterator.hasNext()) {
			  
			  com.amazonaws.services.kinesis.model.Record record = recordIterator.next();
			  ByteBuffer byteBuffer = record.getData();
			  logger.info(" record data : " +  StandardCharsets.UTF_8.decode(byteBuffer).toString()
					  + " partition key  : " + record.getPartitionKey()  
		+	" Arrival time : " + record.getApproximateArrivalTimestamp());
			  
			  
		  }
		  
		
		  
		  try {
		    Thread.sleep(1000);
		  } 
		  catch (InterruptedException exception) {
		    throw new RuntimeException(exception);
		  }
		  
		  shardIterator = result.getNextShardIterator();
		}
		

	}

}
