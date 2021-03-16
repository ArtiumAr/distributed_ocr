# distributed_ocr

This an application I wrote that applies OCR to images in a distributed manner. The server runs on AWS EC2 instances, 
with the clients using SQS queues to request jobs to be completed by the server. The result is then uploaded to S3.

Usage

You need to create a Worker AMI and a manager AMI(and supply them to the local client), which consist of Ubuntu instances with the respective JAR uploaded to the home dir, with Java installed on both and Tesseract installed on the Worker AMI. 
To use the client you need to provide it with the Manager/Worker AMIs. 

To run the client enter the following command:
java -jar yourjar.jar inputFileName outputFileName n managerAmId workerAmId terminate

where:
- inputFileName is a file with URLs to the images you would like to apply OCR to
- outputFileName is an HTML formatted file consisting of the strings returned by the OCR app
- n is the number of links you'd like a single worker to process(on average)
- terminate(can be any string) signals to the manager this will be the last job he will receive. If you'd like to run
  multiple jobs, simply withhold the terminate message until the last job you want to run. 
