package com.pwb.pulsar;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;

public class simplePulsarConsumer {

	private static final String SERVICE_URL = "pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651";
	private static final String YOUR_PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTc5MTUzMDQsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzRhYmY1MzA4LWE1MTUtNGZhOS1iYWVjLTk0ZDE5M2U2ZjA5YztjSGRpWlhoaGJYQnNaUT09O2Y5NDAyYTU5MDEiLCJ0b2tlbmlkIjoiZjk0MDJhNTkwMSJ9.XjS7g_kGcnEuRnK4IeRazfP64YJZCZlAadqmQf3j2GQt11fMwzkIYSe0LUPzhTbbRYEOGIEULKvekjBLgnn2Wnx0Lhw-DS5yvYwLnFlL5fH3XefiYOK9E3yxH157kM1defVo18hVeRvLR_347KuldyRcyrObmSxrEswbak6ZfyfT3LsNRw1jLISo8OA_fells6M9OkUwwUVOtNvOzsJauXoN2pNuIZ-5KHnJQrN7F2kN2YAg6xGRjbWHSGP7h1QBi5sbV6h4jAcAfGI97eHOGUsLtR48B-oaJ34pdtRhnLMlPiuZDnaGYiue187e9LCk092Q2MhdQYLiNvA95ouKfg";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                    AuthenticationFactory.token(YOUR_PULSAR_TOKEN)
                )
                .build();

        // Create consumer on a topic with a subscription
        Consumer consumer = client.newConsumer()
                .topic("pwbexample/iot-sensor/sensor-temps")
                .subscriptionName("test-subscription")
                .subscribe();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                System.out.printf("Message received: %s \n", new String(msg.getData()));

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();

        // Close the client
        client.close();

    }
}
