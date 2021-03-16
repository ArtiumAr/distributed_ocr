package com.Manager;


import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DataSingleton {
    ConcurrentHashMap<Integer, String> local_queues_urls;
    ConcurrentHashMap<Integer, Integer> local_queues_number_of_urls;
    ConcurrentLinkedQueue<Pair<String, Integer>> URLs_file_path_owner_pair_queue;
    LinkedList<Pair<String, Integer>> URL_owner_pair_queue;
    LinkedList<Pair<Integer, Pair<String, String>>> finished_url_readings_queue;
    ConcurrentHashMap<Integer, Integer> owner_number_of_workers_required_map;
    final String bucket = "bucket-5rsowjzb2sn5h2281391857292017495237234348590203423411123";
    final String main_queue_name = "queue-5rsowjzb2sn5h22813918572920174952372343485902034234111239";
    String amID = "";
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean local_connection_exists = new AtomicBoolean(false);
    AtomicInteger total_urls_to_send = new AtomicInteger(0);
    String main_queue_url;
    int current_owner_id_processed;


    private DataSingleton(){
        local_queues_urls = new ConcurrentHashMap<>();
        local_queues_number_of_urls = new ConcurrentHashMap<>();
        URLs_file_path_owner_pair_queue = new ConcurrentLinkedQueue<>();
        URL_owner_pair_queue = new LinkedList<>();
        finished_url_readings_queue = new LinkedList<>();
        owner_number_of_workers_required_map = new ConcurrentHashMap<>();
    }


    private static class DataSingletonSingleton{
        private static final DataSingleton instance = new DataSingleton();
    }

    public static synchronized DataSingleton getInstance(){
        return DataSingletonSingleton.instance;
    }

}
