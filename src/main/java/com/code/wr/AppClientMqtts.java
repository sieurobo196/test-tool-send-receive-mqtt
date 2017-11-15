/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.code.wr;

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.log4j.Logger;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 *
 * @author NXCOMM
 */
public class AppClientMqtts {
    
    private static final Logger logger = Logger.getLogger(AppClientMqtts.class);
    static String MQTT_URL = "ssl://localhost:8883";
    
    public static void main(String args[]) {
        AppClientMqtts test_MQTTS = new AppClientMqtts();
        test_MQTTS.ConnectCamAapp();
    }
    
    public BlockingConnection getConnect(String clientId, String userName, String password, String udid) {
        try {
            MQTT mqtt = new MQTT();
            mqtt.setHost(MQTT_URL);
            mqtt.setCleanSession(true);
            mqtt.setUserName(userName);
            mqtt.setPassword(password);
            mqtt.setClientId(clientId);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
            mqtt.setSslContext(sslContext);
            BlockingConnection subscribeConnection = mqtt.blockingConnection();
            try {
                subscribeConnection.connect();
                logger.info("connect : " + udid + " " + subscribeConnection.isConnected());
                
            } catch (Exception exception) {
                logger.error("error connect mqtts" + exception);
            }
            
            return subscribeConnection;
        } catch (Exception e) {
            logger.error("Error Init Mqtt Connection " + e);
            return null;
        }
        
    }
    
    public void ConnectCamAapp() {
        logger.info("start ConnectCamAapp");
        String appConf = "d9f82770c071c4b100103ad03e79998ad862e45f5238de923641c8cf5fef2b65,"
                + "5e552049af0399c8a71da9da7480ba5fbe5675e79548df058107c1b36a3a09f8,"
                + "eacf750b0ab3ee76f9f78c76e34bd11ba1e9e05138dc96d4226885b36ac5bfa3,"
                + "app_gcm/a4b1dea25f4e86c330e2e8684a90e51e513ecfdd2c3edecc1d06d9f8a54c81ed/sub,"
                + "device_001/e3540cfa0e3d8e14c0777ca7bc17c86ef5b409a1da30a14b35a470f5d4b895c9/sub,"
                + "01100114999CHMQTTSCAMAPP";
        AppClient appClient = new AppClient(appConf);
        appClient.start();
        
    }
    
    static class DefaultTrustManager implements X509TrustManager {
        
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
    
    class PublicMessage extends Thread {
        
        BlockingConnection subConnection = null;
        String topicCam = "";
        String clientApp = "";
        String topicApp = "";
        String udid = "";
        
        public PublicMessage(BlockingConnection blockingConnection, String topicCamPub, String clientId, String topicAppSub, String udid) {
            subConnection = blockingConnection;
            topicCam = topicCamPub;
            clientApp = clientId;
            topicApp = topicAppSub;
            this.udid = udid;
        }
        
        @Override
        public void run() {
            
            int count = 0;
            try {
                
                QoS qoSSubscrice = QoS.AT_MOST_ONCE;
                
                Topic topic = new Topic(topicApp, qoSSubscrice);
                Topic[] topics = {topic};
                subConnection.subscribe(topics);
                
                long startTime = System.currentTimeMillis();
                while (true) {
                    
                    String msg = "";
                    msg = "2app_topic_sub=" + topicApp + "&time=" + System.currentTimeMillis() + "&req=get_session_key&mode=remote&port1=50915&ip=115.78.13.114&streamname=CC3A61D7DEC6_50915 ";
                    
                    QoS qoSPublish = QoS.AT_MOST_ONCE;
                    
                    long startSendMsg = System.currentTimeMillis();
                    subConnection.publish(topicCam, (msg + count).getBytes(), qoSPublish, false);
                    count++;
                    logger.info("App publish: "+msg);
                    Thread.sleep(1000);
                    
                }
            } catch (Exception ex) {
                
                logger.error("error publish messsage " + ex);
            }
            logger.info(clientApp + " total send: " + count);
            
        }
    }
    
    class ReceiveMessage extends Thread {
        
        BlockingConnection subConnection = null;
        String topicSub = "";
        String clientIdRecevie = "";
        long startTime;
        String udid = "";
        
        public ReceiveMessage(BlockingConnection blockingConnection, String topicApp, String clientId, String udid) {
            subConnection = blockingConnection;
            clientIdRecevie = clientId;
            topicSub = topicApp;
            this.udid = udid;
        }
        
        @Override
        public void run() {
            
            try {
                
                logger.info("subscribe topic " + topicSub);
                logger.info("wait received message from " + topicSub);
                int count = 0;
                while (true) {
                    Message message = subConnection.receive();
                    String responseMsg = new String(message.getPayload());
                    logger.info("App receive :" + responseMsg);
                    count++;
                    logger.info("count: " + count);
                }
                
            } catch (Exception ex) {
                logger.error("error timeout received " + ex);
            }
        }
    }
    
    class AppClient extends Thread {
        
        String clientId = "";
        String userName = "";
        String password = "";
        String topicApp = "";
        String topicCam = "";
        String udid = "";
        public long totalLatency = 0;
        public int totalReceive = 0;
        String formatAppConf = ",";
        
        public AppClient(String appClient) {
            clientId = appClient.split(formatAppConf)[0];
            userName = appClient.split(formatAppConf)[1];
            password = appClient.split(formatAppConf)[2];
            topicApp = appClient.split(formatAppConf)[3];
            topicCam = appClient.split(formatAppConf)[4];
            udid = appClient.split(formatAppConf)[5];
        }
        
        @Override
        public void run() {
            
            BlockingConnection connection = getConnect(clientId, userName, password, udid);
//            ReceiveMessage receiveMessage = new ReceiveMessage(connection, topicApp, clientId, udid);
//            receiveMessage.start();
            PublicMessage publicMessage = new PublicMessage(connection, topicCam, clientId, topicApp, udid);
            publicMessage.start();
            
        }
    }
    
}
