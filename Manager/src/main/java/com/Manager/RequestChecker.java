package com.Manager;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static java.lang.Thread.yield;

public class RequestChecker implements Runnable {
    String local_applications_queue_url;
    SqsClient sqs;
    DataSingleton dataSingleton;
    S3Client s3;
    int number_of_locals;
    HashMap<String, String> locals;


    public RequestChecker(String local_applications_queue_url, SqsClient sqs, S3Client s3) {
        this.local_applications_queue_url = local_applications_queue_url;
        this.sqs = sqs;
        this.dataSingleton = DataSingleton.getInstance();
        this.s3 = s3;
        number_of_locals = 0;
        this.locals = new HashMap<>();
    }

    public void run(){
        //wait for and receive client job requests
        String cwd = System.getProperty("user.dir");
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(dataSingleton.main_queue_name)
                .build();
        String main_queue_url = sqs.getQueueUrl(getQueueRequest).queueUrl();
        dataSingleton.main_queue_url = main_queue_url;
        JSONParser parser = new JSONParser();
        while(!dataSingleton.terminated.get()) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(main_queue_url)
                    .waitTimeSeconds(15)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for(Message m : messages){
                if(!dataSingleton.terminated.get()) {
                    dataSingleton.local_connection_exists.compareAndSet(false, true);
                    try {
                        JSONObject client_request = (JSONObject) parser.parse(m.body());
                        String name = (String) client_request.get("name");
                        if (name.equals("termination message")) {
                            dataSingleton.terminated.compareAndSet(false, true);
                        } else {
                            String local_queue_url = (String) client_request.get("queue_url");
                            String number_of_lines = (String) client_request.get("lines");
                            dataSingleton.total_urls_to_send.getAndAdd(Integer.parseInt(number_of_lines));
                            if(!locals.containsKey(local_queue_url)) {
                                locals.putIfAbsent(local_queue_url, "");
                                number_of_locals++;
                                String key = (String) client_request.get("key_location");
                                String path = cwd + "/local_client" + number_of_locals + ".txt";
                                String n = (String) client_request.get("images_per_worker");
                                int workers_required = Integer.parseInt(number_of_lines) / Integer.parseInt(n);
                                dataSingleton.owner_number_of_workers_required_map.put(number_of_locals, workers_required);
                                s3.getObject(GetObjectRequest.builder().bucket(dataSingleton.bucket).key(key).build(),
                                        ResponseTransformer.toFile(Paths.get(path)));
                                s3.deleteObject(DeleteObjectRequest.builder()
                                        .bucket(dataSingleton.bucket)
                                        .key(key)
                                        .build());
                                dataSingleton.URLs_file_path_owner_pair_queue.add(new Pair<>(path, number_of_locals));
                                dataSingleton.local_queues_urls.putIfAbsent(number_of_locals, local_queue_url);
                                dataSingleton.local_queues_number_of_urls.putIfAbsent(number_of_locals, Integer.parseInt(number_of_lines));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(local_applications_queue_url)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);
                }
            }
            yield();
        }
    }

}
