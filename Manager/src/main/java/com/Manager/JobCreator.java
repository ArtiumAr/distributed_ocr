package com.Manager;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

import static java.lang.Thread.yield;


public class JobCreator implements Runnable{
    DataSingleton dataSingleton;
    int n;
    SqsClient sqs;
    int number_of_workers;
    int number_of_urls_sent;
    HashMap<Integer, JSONObject> summary_map;
    HashMap<Integer, Integer> number_of_urls_in_summary;
    S3Client s3;
    ArrayList<String> instances;
    int urls_sent;
    int workers_created;
    LinkedList<SendMessageBatchRequestEntry> batch_entries;


    public JobCreator(S3Client s3, SqsClient sqs) {
        dataSingleton = DataSingleton.getInstance();
        this.sqs = sqs;
        number_of_workers = 0;
        number_of_urls_sent = 0;
        summary_map = new HashMap<>();
        number_of_urls_in_summary = new HashMap<>();
        this.s3=s3;
        urls_sent = 0;
        workers_created = 0;
        instances = new ArrayList<>();
        batch_entries = new LinkedList<>();
    }

    public void run(){
        Ec2Client ec2 = Ec2Client.create();
        String task_queue_name = "task-queue-5rsowjzb2sn5h22813918572920174";
        String task_queue_url = createQueue(task_queue_name);
        String worker_results_queue_name = "result-queue-5rsowjzb2sn5h22813918572920174";
        String worker_results_queue_url = createQueue(worker_results_queue_name);
        //yield until a client connection
        while(!dataSingleton.local_connection_exists.get())
            yield();
        while((!dataSingleton.terminated.get()) || (number_of_urls_sent < dataSingleton.total_urls_to_send.get())) {
            //sequentially process a job request received from a client
            //work on results from multiple clients (as the workers send them)
            receive_request();
            create_workers_send_worker_tasks(ec2 ,task_queue_url);
            check_and_process_results(worker_results_queue_url);
            package_and_send_results();
            monitor_instances(ec2, dataSingleton.amID); //check and restart crashed workers
            yield();
        }
        ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instances).build());
        cleanUp(s3, sqs, task_queue_url, worker_results_queue_url);
    }

    public String create_instance(String name, String amiId, Ec2Client ec2) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn("arn:aws:iam::952244235589:instance-profile/Manager")
                        .build())
                .userData(Base64.getEncoder().encodeToString(
                        ("#!/bin/bash\n\n" +
                                "echo export AWS_DEFAULT_REGION=\"us-east-1\" >> /etc/profile\n" +
                                "cd ~\n" +
                                "java -jar /home/ubuntu/Worker.jar &\n").getBytes()))
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

    public String createQueue(String queue_name){
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }


    public void cleanUp(S3Client s3,SqsClient sqs, String task_queue_url,String result_queue_url) {
        DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder()
                .queueUrl(dataSingleton.main_queue_url)
                .build();
        sqs.deleteQueue(deleteRequest);
        DeleteQueueRequest deleteRequest2 = DeleteQueueRequest.builder()
                .queueUrl(task_queue_url)
                .build();
        sqs.deleteQueue(deleteRequest2);
        DeleteQueueRequest deleteRequest3 = DeleteQueueRequest.builder()
                .queueUrl(result_queue_url)
                .build();
        sqs.deleteQueue(deleteRequest3);
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(dataSingleton.bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    public void monitor_instances(Ec2Client ec2, String amiId){
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .instanceIds(instances)
                .build();
        DescribeInstancesResponse status_response = ec2.describeInstances(describeInstancesRequest);
        for(Reservation res : status_response.reservations()){
            for(Instance ins: res.instances()){
                if(ins.state().name().toString().equals("terminated") ||
                        ins.state().name().toString().equals("shutting-down")){
                    instances.remove(ins.instanceId());
                    String replacement_instance = create_instance("replacement", amiId, ec2);
                    instances.add(replacement_instance);
                }
            }
        }
    }


    public void receive_request(){
        if (!dataSingleton.URLs_file_path_owner_pair_queue.isEmpty()) {
            Pair<String, Integer> pair = dataSingleton.URLs_file_path_owner_pair_queue.poll();
            String path = pair.getKey();
            int local_id = pair.getValue();
            dataSingleton.current_owner_id_processed = local_id;
            try {
                Scanner scanner = new Scanner(new File(path));
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    dataSingleton.URL_owner_pair_queue.add(new Pair<>(line, local_id));
                }
            } catch (Exception e) {
                System.out.println("error while opening the following file: " + path);
            }
        }
    }

    public void create_workers_send_worker_tasks(Ec2Client ec2, String task_queue_url){
        int workers_required = 0; //avoid unnecessary queries to concurrenthashmap
        while(!dataSingleton.URL_owner_pair_queue.isEmpty()){
            Pair<String, Integer> pair = dataSingleton.URL_owner_pair_queue.poll();
            String url = pair.getKey();
            int owner_id = pair.getValue();
            if(workers_required == 0) {
                workers_required =
                        dataSingleton.owner_number_of_workers_required_map.get(owner_id);
            }
            int workers_to_create =
                    workers_required - workers_created;
            JSONObject worker_task = new JSONObject();
            worker_task.put("name", "new image task");
            worker_task.put("URL", url);
            worker_task.put("owner_id", Integer.toString(owner_id));
            worker_task.put("number_of_images_to_process", Integer.toString(n));
            batch_entries.add(SendMessageBatchRequestEntry.builder()
                    .messageBody(worker_task.toJSONString())
                    .id(Integer.toString(batch_entries.size()))
                    .build());
            if(batch_entries.size() == 10){
                SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                        .queueUrl(task_queue_url)
                        .entries(batch_entries)
                        .build();
                sqs.sendMessageBatch(send_batch_request);
                batch_entries.clear();
            }
            number_of_urls_sent++;
            if(workers_to_create > 0) {
                String instance_id = create_instance(Integer.toString(number_of_workers++), dataSingleton.amID, ec2);
                instances.add(instance_id);
                workers_created++;
            }
        }
        if(!batch_entries.isEmpty()){ //if <10 entries remain at the end of the job
            int urls_remain = batch_entries.size();
            SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                    .queueUrl(task_queue_url)
                    .entries(batch_entries)
                    .build();
            sqs.sendMessageBatch(send_batch_request);
            batch_entries.clear();
            number_of_urls_sent += urls_remain;
        }
    }

    public void check_and_process_results(String worker_results_queue_url){
        JSONParser parser = new JSONParser();
        boolean empty_result_queue = false;
        while(!empty_result_queue){
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(worker_results_queue_url)
                    .maxNumberOfMessages(10)
                    .visibilityTimeout(60)
                    .waitTimeSeconds(15)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            if(messages.isEmpty())
                empty_result_queue = true;
            for(Message m : messages){
                try {
                    JSONObject obj = (JSONObject) parser.parse(m.body());
                    String text = (String) obj.get("text");
                    String url = (String) obj.get("URL");
                    Pair<String, String> url_text = new Pair<>(url, text);
                    int owner_id = Integer.parseInt((String) obj.get("owner_id"));
                    dataSingleton.finished_url_readings_queue.add(new Pair<>(owner_id, url_text));
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .receiptHandle(m.receiptHandle())
                            .queueUrl(worker_results_queue_url)
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);

                }catch (Exception e){}
            }
        }
    }

    public void package_and_send_results(){
        JSONParser summary_parser = new JSONParser();
        while(!dataSingleton.finished_url_readings_queue.isEmpty()) {
            Pair<Integer, Pair<String, String>> url_text_pair = dataSingleton.finished_url_readings_queue.poll();
            int owner_id = url_text_pair.getKey();
            String url = url_text_pair.getValue().getKey();
            String text = url_text_pair.getValue().getValue();
            String file_name = System.getProperty("user.dir") + "/summary" + owner_id + ".json";
            if (number_of_urls_in_summary.containsKey(owner_id)) {
                try {
                    JSONObject url_result = (JSONObject) summary_parser.parse(new FileReader(file_name));
                    url_result.put(url, text);
                    BufferedWriter writer =
                            new BufferedWriter(new FileWriter(
                                    file_name));
                    writer.write(url_result.toJSONString());
                    writer.close();
                } catch (Exception e) {
                    System.out.println("failed to open file " + file_name);
                }
                int number_of_urls = number_of_urls_in_summary.get(owner_id);
                number_of_urls_in_summary.put(owner_id, ++number_of_urls);
            } else {
                JSONObject summary = new JSONObject();
                summary.put(url, text);
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(file_name));
                    writer.write(summary.toJSONString());
                    writer.close();
                } catch (Exception e) {
                    System.out.println("failed to open file " + file_name);
                }
                number_of_urls_in_summary.put(owner_id, 1);
            }
            if (number_of_urls_in_summary.get(owner_id)
                    .equals(dataSingleton.local_queues_number_of_urls.get(owner_id))) {
                number_of_urls_in_summary.remove(owner_id);
                File file = new File(file_name);
                s3.putObject(PutObjectRequest.builder().bucket(dataSingleton.bucket).key("summary" + owner_id).build(),
                        RequestBody.fromFile(file));
                file.delete();
                JSONObject termination_msg = new JSONObject();
                termination_msg.put("name", "done task");
                termination_msg.put("key", "summary" + owner_id);
                SendMessageRequest send_msg_request = SendMessageRequest.builder()
                        .queueUrl(dataSingleton.local_queues_urls.get(owner_id))
                        .messageBody(termination_msg.toJSONString())
//                            .delaySeconds(5)
                        .build();
                sqs.sendMessage(send_msg_request);
            }
        }
    }
}
