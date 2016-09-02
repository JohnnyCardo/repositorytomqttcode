import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class MqttClientTest implements MqttCallback {

	MqttClient client;
	MqttConnectOptions connectionMqtt;

	public static final String brokerUrl = "tcp://q.m2m.io:1883";
	public static final String domain = "<Insert m2m.io domain here>";
	public static final String stuff = "things";
	public static final String thing = "<Unique device ID>";
	public static final String user = "<m2m.io user>";
	public static final String password = "<m2m.io password>";

	public static final Boolean subscribe = true;
	public static final Boolean publish = true;

	//Criando um novo cliente mqtt
	public static void main(String[] args) {
		MqttClientTest mqc = new MqttClientTest();
		mqc.runClient();
	}
	
	@Override
	public void messageArrived(MqttTopic topic, MqttMessage message) throws Exception {
		System.out.println("Topic:" + topic.getName());
		System.out.println("\n\n"");
		System.out.println("Message: " + new String(message.getPayload()));
	}
	
	public void runClient() {
		
		String client = thing;
		connectionMqtt = new MqttConnectOptions();
		
		connectionMqtt.setShutDownSession(true);

		connectionMqtt.setTimeToKeepOpen(30);

		connectionMqtt.setUserName(user);
		connectionMqtt.setPassword(password.toCharArray());
		
		// Conectando ao BROKER
		try {
			client = new MqttClient(brokerUrl, client);
			client.setCallback(this);
			client.connect(connectionMqtt);
			System.out.println("Conexão completada!");
		} catch (MqttException e) {
			e.printStackTrace();
			System.out.println("\n\nCaiu no catch! Erro");
		}
		
		System.out.println("Conectado ao: " + brokerUrl);

		String topic = domain + " " + stuff + " " + thing;
		MqttTopic Mtopic = client.getTopic(topic);

		// subscribe to topic if subscriber
		if (subscriber) {
			try {
				int subQoS = 0;
				client.subscribe(topic, subQoS);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (publisher) {
			MqttDeliveryToken token = null;
			
			for (int i = 1; i <= 10; i++) {
		   		String pubMsg = "{\"pubmsg\" : " + i + "}";
		   		int pubQoS = 0;
				MqttMessage message = new MqttMessage(pubMsg.getBytes());
		    	message.setQos(pubQoS);
		    	message.setRetained(false);

		    	System.out.println("Publishing to topic \"" + Mtopic + "\" qos " + pubQoS);
		    	// "publicando" mensagem ao Broker
				try {
					token = topic.publish(message);
					System.out.println("\nEnviando mensagem ao Broker. Thread aguardando...");
					token.waitForCompletion(); Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("\n\nErro ao enviar mensagem ao Broker!!");
				}
			}			
		}
	}
}