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
public class CamClientMqtts {

    private static final Logger logger = Logger.getLogger(CamClientMqtts.class);

    static String MQTT_URL = "ssl://mqtt.staging.l-kloud.com:8884";

    public static void main(String args[]) {
        CamClientMqtts test_MQTTS = new CamClientMqtts();
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

            //if (subscribeConnection == null) {
            BlockingConnection subscribeConnection = mqtt.blockingConnection();
            try {
                subscribeConnection.connect();
                logger.info("connect : " + udid + " " + subscribeConnection.isConnected());
            } catch (Exception exception) {
                logger.error("error connect mqtts " + udid, exception);
            }
            //}
            return subscribeConnection;
        } catch (Exception e) {
            logger.error("Error Init Mqtt Connection " + udid, e);
            return null;
        }

    }

    public void ConnectCamAapp() {
        logger.info("start ConnectCamAapp");
//        for (String camConf : listCamConf) {
        String camConf = "85cb37d6964030ad0a230e47e86088fd4c1991fd3b3af6d68176589e2f3c39ee,"
                + "526e1ea6cace6c3f3e4158feecf75b296657b9ccb8ba2680e7b89ad7ed341cca,"
                + "0a0cd037685e347d5c81e288fa38f78f908887fc6c9e5adafdb6f75711d25230,"
                + "device_001/e3540cfa0e3d8e14c0777ca7bc17c86ef5b409a1da30a14b35a470f5d4b895c9/sub,"
                + "01100114999CHMQTTSCAMAPP";
        logger.info("start " + camConf);
        CamClient camClient = new CamClient(camConf);
        camClient.start();
//        }

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

    class ReceiveMessage extends Thread {

        BlockingConnection subConnection = null;
        String topicReceive = "";
        String clientIdRecevie = "";
        String UDID = "";

        public ReceiveMessage(BlockingConnection blockingConnection, String topic, String userName, String udid) {
            subConnection = blockingConnection;
            topicReceive = topic;
            clientIdRecevie = userName;
            UDID = udid;
        }

        @Override
        public void run() {
            QoS qoSSubscrice = QoS.AT_MOST_ONCE;

            QoS qoSPublish = QoS.AT_MOST_ONCE;

            Topic topic = new Topic(topicReceive, qoSSubscrice);
            Topic[] topics = {topic};
            try {
                byte[] byteReceive = subConnection.subscribe(topics);
                logger.info("subscribe topic " + topicReceive);
                logger.info("wait received message from topic " + topicReceive);
                int count = 0;
                while (true) {
                    Message message = subConnection.receive();
                    String responseMsg = new String(message.getPayload());
                    logger.info("Cam receive :" + responseMsg);

                    String timeSend = responseMsg.split("&")[1].split("=")[1];
                    String topicAppSub = responseMsg.split("&")[0].split("=")[1];
                    String req = responseMsg.split("&")[2].split("=")[1];
                    String msg = "3id: " + topicReceive + "&time:" + timeSend + "&get_session_key: error=200,port1=443&ip=115.78.13.114&key=2f7d572a513f382e575c617c73744234&sip=rss1.staging.l-kloud.com&sp=8000&rn=5c43433a2a7278354f573750";

//                    subConnection.publish(topicAppSub, msg.getBytes(), qoSPublish, false);
                    count++;
                    logger.info("count " + count);

                }

            } catch (Exception ex) {
                logger.error("error subscribe topic " + ex);
            }
        }
    }

    class CamClient extends Thread {

        String clientId = "";
        String userName = "";
        String password = "";
        String topicCam = "";
        String udid = "";
        String formatCamConf = ",";

        public CamClient(String camConf) {
            clientId = camConf.split(formatCamConf)[0];
            userName = camConf.split(formatCamConf)[1];
            password = camConf.split(formatCamConf)[2];
            topicCam = camConf.split(formatCamConf)[3];
            udid = camConf.split(formatCamConf)[4];

        }

        @Override
        public void run() {

            BlockingConnection connection = getConnect(clientId, userName, password, udid);
            if (connection != null) {
                ReceiveMessage receiveMessage = new ReceiveMessage(connection, topicCam, userName, udid);
                receiveMessage.start();
                int i = 0;
                while (true) {
                    if (!connection.isConnected()) {
                        if (i == 0) {
                            logger.info("cam disconnect");
                            i = 1;
                        }
                    } else if (i == 1) {
                        logger.info("cam connect");
                        i = 0;
                    }

                }
            }
        }
    }

}
