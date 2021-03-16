package com.Worker;
import net.sourceforge.tess4j.util.LoadLibs;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URL;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;


public class Worker {
    public static void main(String[] args ){
        String task_queue_name = "task-queue-5rsowjzb2sn5h22813918572920174";
        String result_queue_name = "result-queue-5rsowjzb2sn5h22813918572920174";
        int images_processed = 0;
        Region region = Region.US_EAST_1;
        SqsClient sqs = SqsClient.builder().region(region).build();
        String task_queue_url = getQueueURL(sqs, task_queue_name);
        String result_queue_url = getQueueURL(sqs, result_queue_name);
        JSONParser parser = new JSONParser();
        while(true) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(task_queue_url)
                    .visibilityTimeout(60)
                    .waitTimeSeconds(15)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            if (!messages.isEmpty()) {
                try {
                    JSONObject task = (JSONObject) parser.parse(messages.get(0).body());
                    String url = (String) task.get("URL");
                    String path = retrieveImage(url, images_processed++);
                    String owner_id = (String) task.get("owner_id");
                    String text = applyOCR(path);
                    JSONObject task_result = new JSONObject();
                    task_result.put("name", "done OCR task");
                    task_result.put("URL", url);
                    task_result.put("text", text);
                    task_result.put("owner_id", owner_id);
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(result_queue_url)
                            .messageBody(task_result.toJSONString())
                            .build();
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .receiptHandle(messages.get(0).receiptHandle())
                            .queueUrl(task_queue_url)
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);
                    sqs.sendMessage(send_msg_request);
                } catch (ParseException pe) {
                    pe.printStackTrace();
                }
            }
        }
    }

    public static String getQueueURL(SqsClient sqs, String queue_name){
        GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        GetQueueUrlResponse queueUrlResponse = sqs.getQueueUrl(queueUrlRequest);
        return queueUrlResponse.queueUrl();
    }


    public static String retrieveImage(String string_url, int i){
        String cwd = System.getProperty("user.dir");
        BufferedImage image =null;
        try{
            URL url =new URL(string_url);
            image = ImageIO.read(url);
            String path = cwd + "/" + "cur_img" + i + ".png";
            ImageIO.write(image, "png",new File(path));
            return path;
        }catch(Exception e){
            return "";
        }
    }

    public static String applyOCR(String path){
        if(path.equals("")){
            return "bad link";
        }
        Tesseract tesseract = new Tesseract();
        File tessDataFolder = LoadLibs.extractTessResources("tessdata");
        tesseract.setDatapath(tessDataFolder.getAbsolutePath());
        try {
            return tesseract.doOCR(new File(path));
        } catch (TesseractException e) {
            e.printStackTrace();
            return "failed OCR";
        }
    }
}
