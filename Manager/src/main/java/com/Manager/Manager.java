package com.Manager;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

public class Manager {
    public static void main(String[] args){
        S3Client s3 = S3Client.builder().build();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        String local_applications_queue_name = "queue-5rsowjzb2sn5h22813918572920174952372343485902034234111239";
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(local_applications_queue_name)
                .build();
        DataSingleton.getInstance().amID = args[0];
        String local_applications_queue_url = sqs.getQueueUrl(getQueueRequest).queueUrl();
        Thread checker_thread = new Thread(new RequestChecker(local_applications_queue_url, sqs ,s3));
        checker_thread.start();
        //2 threads
        Thread job_creator_thread = new Thread(new JobCreator(s3, sqs));
        job_creator_thread.start();
    }
}


