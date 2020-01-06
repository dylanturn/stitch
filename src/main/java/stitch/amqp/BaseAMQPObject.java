package stitch.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Logger;
import stitch.util.BaseObject;
import stitch.util.properties.MongoPropertyStore;
import stitch.util.properties.PropertyStore;

import java.net.URI;

public abstract class BaseAMQPObject extends BaseObject implements AutoCloseable {
    static final Logger logger = Logger.getLogger(BaseAMQPObject.class);
    protected String routeKey;
    private String amqpUsername;
    private String amqpPassword;
    private String amqpHost;
    private int amqpPort;
    private String amqpExchange;

    private Connection connection;
    private Channel channel;
    private Object monitor;

    public BaseAMQPObject(String prefix, String id){
        super(prefix, id);
        // Get the properties for the AMQP connection.
        PropertyStore propertyStore = new MongoPropertyStore();
        String secretKey = "";
        String secretSalt = "";
        this.amqpHost = propertyStore.getString("stitch/amqp", "host");
        this.amqpPort = propertyStore.getInt("stitch/amqp", "port");
        this.amqpUsername = propertyStore.getString("stitch/amqp", "username");
        this.amqpPassword = propertyStore.getSecret("stitch/amqp", "password", secretKey, secretSalt);
        this.amqpExchange = propertyStore.getString("stitch/amqp", "exchange");
        propertyStore.close();
        this.monitor = new Object();

        // Connect to the AMQP endpoint
        try{
            ConnectionFactory factory = new ConnectionFactory();
            URI amqpURI = URI.create(String.format("stitch.amqp://%s:%s@%s/%s", amqpUsername, amqpPassword, amqpHost, amqpUsername));
            factory.setUri(amqpURI);
            connection = factory.newConnection();
            channel = connection.createChannel();
            logger.debug("AMQP connected!");
        } catch(Exception error){
            logger.error(String.format("Failed to connect to the AMQP host: ",amqpHost), error);
        }
    }

    public String getHost(){
        return amqpHost;
    }
    public String getExchange(){
        return amqpExchange;
    }
    public Channel getChannel() {
        return channel;
    }
    public Object getMonitor() {
        return monitor;
    }

    // TODO: Maybe decide if there are other things that need to happen for it to close?
    @Override
    public void close() throws Exception {
        channel.close();
        connection.close();
    }
}