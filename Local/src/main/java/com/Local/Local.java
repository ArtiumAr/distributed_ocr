package com.Local;

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import j2html.TagCreator;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class Local {

    public static void main(String[] args) throws IOException {

        Region region = Region.US_EAST_1;
        String queue_name = "queue-5rsowjzb2sn5h22813918572920174952372343485902034234111239";
        String local_queue_name = "local-queue-" +  System.currentTimeMillis();
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        S3Client s3 = S3Client.builder().build();
        Ec2Client ec2 = Ec2Client.create();
        String amId = "ami-0addf3194abe7b95c";
        String bucket = "bucket-5rsowjzb2sn5h2281391857292017495237234348590203423411123";
        String key = "images" + local_queue_name;
        String number_of_workers = args[2];
            File input_file = new File(args[0]);
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        int number_of_URLs = 0;
        while (reader.readLine() != null) number_of_URLs++;
        reader.close();
        BucketSetup(s3, bucket);
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(input_file));
        create_instance(args[0], amId, "", ec2);
        String queue_url = createQueue(queue_name, sqs);
        JSONObject ocr_task = new JSONObject();
        String local_queue_url = createQueue(local_queue_name, sqs);
        ocr_task.put("name", "new task");
        ocr_task.put("key_location", key);
        ocr_task.put("lines", Integer.toString(number_of_URLs));
        ocr_task.put("images_per_worker", number_of_workers);
        ocr_task.put("queue_url", local_queue_url);
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queue_url)
                .messageBody(ocr_task.toJSONString())
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
        String summary_key = checkFinishedMessage(local_queue_url, sqs);
        createHTML(s3, bucket, args[1], summary_key);
        if(args.length == 4) {
            JSONObject termination_msg = new JSONObject();
            termination_msg.put("name", "termination message");
            termination_msg.put("termination_message", args[3]);
            termination_msg.put("lines", "0");
            send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queue_url)
                    .messageBody(termination_msg.toJSONString())
                    .delaySeconds(5)
                    .build();
            sqs.sendMessage(send_msg_request);
        }
        cleanUp(sqs, local_queue_url, ec2);
    }

    public static String checkFinishedMessage(String queue_url, SqsClient sqs){
        boolean terminated = false;
        String key = "";
        while (!terminated) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queue_url)
                    .maxNumberOfMessages(5)
                    .visibilityTimeout(60)
                    .waitTimeSeconds(15)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            JSONParser parser = new JSONParser();
            for (Message m : messages) {
                try {
                    JSONObject obj = (JSONObject) parser.parse(m.body());
                    String name = (String) obj.get("name");
                    if (name.equals("done task")) {
                        terminated = true;
                        key = (String) obj.get("key");
                    }
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .receiptHandle(m.receiptHandle())
                            .queueUrl(queue_url)
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);
                }catch(ParseException pe){}
            }
        }
        return key;
    }

    public static void createHTML(S3Client s3, String bucket, String output_file_name, String key){
        String cwd = System.getProperty("user.dir");
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(Paths.get(cwd + "/summary.json")));
        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
        try {
            JSONParser parser = new JSONParser();
            String file_string = Files.lines(Paths.get(cwd + "/summary.json")).reduce("", (x, y) -> x + y);
            JSONObject jsonObject = (JSONObject)  parser.parse(file_string);
            String html_output = "";
            for(Iterator iterator = jsonObject.keySet().iterator(); iterator.hasNext();) {
                String key1 = (String) iterator.next();
                html_output += j2html.TagCreator.body(
                        TagCreator.img().withSrc(key1),
                        TagCreator.h1((String) jsonObject.get(key1)))
                        .render();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(output_file_name));
            writer.write(html_output);
            writer.close();
            File file = new File(cwd + "/summary.json");
            file.delete();
        } catch (Exception e)
        { }

    }

    public static String createQueue(String queue_name, SqsClient sqs){
        String queue_url = "";
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            sqs.createQueue(request);
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queue_name)
                    .build();
            queue_url = sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (QueueNameExistsException e) {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queue_name)
                    .build();
            queue_url = sqs.getQueueUrl(getQueueRequest).queueUrl();
        }
        return queue_url;
    }

    public static String create_instance(String name, String amiId, String instance_id, Ec2Client ec2) {
        DescribeInstancesResponse status_response = ec2.describeInstances();
        boolean existsRunningManager = false;
        for(Reservation res : status_response.reservations()){
            for(Instance ins: res.instances()){
                if(ins.state().name().toString().equals("running") || ins.state().name().toString().equals("pending")){
                    existsRunningManager = true;
                }
            }
        }
        if(!existsRunningManager) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .arn("arn:aws:iam::952244235589:instance-profile/Manager")
                            .build())
                    .keyName("example_key_pair")
                    .securityGroupIds("sg-0bbb058f9c7b94263")
                    .instanceInitiatedShutdownBehavior("terminate")
                    .userData(Base64.getEncoder().encodeToString(
                            ("#!/bin/bash\n\n" +
                                    "echo export AWS_DEFAULT_REGION=\"us-east-1\" >> /etc/profile\n" +
                                    "cd ~\n" +
                                    "java -jar /home/ubuntu/Manager.jar " + amiId + "\n" +
                                    "shutdown -h now\n").getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();

            Tag tag = Tag.builder()
                    .key("Name")
                    .value(name)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 instance %s based on AMI %s",
                        instanceId, amiId);

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
            System.out.println("Done!");
            return instanceId;
        }
        return "";
    }

    public static boolean checkInstanceStatus(Ec2Client ec2){
        return !ec2.describeInstanceStatus().instanceStatuses().isEmpty(); // False if no active instances
    }

    public static void BucketSetup(S3Client s3Client, String bucketName) {
        //check if bucket exists
        //if it does do nothing, if not build it
        try{
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (Exception e) {
            try {
                s3Client.createBucket(CreateBucketRequest
                        .builder()
                        .bucket(bucketName)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .build())
                        .build());
                s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                        .bucket(bucketName)
                        .build());
            }catch (S3Exception e2){throw e2;}
        }
    }


    public static void cleanUp(SqsClient sqs, String queue_url, Ec2Client ec2) {
        DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder()
                .queueUrl(queue_url)
                .build();
        sqs.deleteQueue(deleteRequest);
    }

}