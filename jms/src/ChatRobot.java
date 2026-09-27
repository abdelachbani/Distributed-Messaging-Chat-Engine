import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import messages.AMessage;
import messages.ChatMessage;
import messages.ConnectOkMessage;
import messages.JoinOkMessage;
import messages.MessageFactory;
import messages.UserJoinsMessage;
import messages.UserLeavesMessage;
import utils_jms.InitialContextLoader;
import utils_jms.JMSChatConfiguration;
import static utils_jms.Destinations.CHANNEL_TOPIC;
import static utils_jms.Destinations.SERVER_QUEUE;
import static utils_jms.Destinations.USER_QUEUE;

/**
 * Headless automated bot client built with JMS.
 * 
 * Subscribes to a specified channel and automatically greets users
 * who join or leave, while replying to private direct messages.
 */
public class ChatRobot {

    public static void main(String[] args) {
        JMSChatConfiguration conf;
        final String nick;
        String myChannelName;
        InitialContext ic;
        ConnectionFactory cfac;
        JMSContext ctx;
        JMSProducer producer;
        JMSConsumer consumer;

        Queue serverQueue;
        Queue myQueue;
        Topic channelTopic;

        conf = JMSChatConfiguration.parse(args);
        if (conf.getNick() == null) nick = "chat-bot"; else nick = conf.getNick();
        if ((myChannelName = conf.getChannelName()) == null) myChannelName = "general";
        String serverUrl = conf.getArtemisUrl(); 
        System.out.println("ChatRobot JMS running. Broker: " + serverUrl + ", nick: " + nick + ", channel: " + myChannelName);

        try {   
            InitialContextLoader.setDefaultArtemisURL(serverUrl);   
            ic = InitialContextLoader.getInitialContext();

            // Connect to message broker and lookup server queue
            cfac = (ConnectionFactory) ic.lookup("ConnectionFactory");
            ctx = cfac.createContext();
            serverQueue = (Queue) ic.lookup(SERVER_QUEUE);

            // Handshake with ChatServer via temporary reply queue
            producer = ctx.createProducer();
            Message connectMsg = ctx.createObjectMessage(MessageFactory.connectMessage(nick));
            Queue tempQueue = (Queue) ctx.createTemporaryQueue();
            connectMsg.setJMSReplyTo(tempQueue);
            producer.send(serverQueue, connectMsg);

            consumer = ctx.createConsumer(tempQueue);
            ObjectMessage connectReply = (ObjectMessage) consumer.receive(2000);
            if (connectReply == null) {
                throw new Exception("ChatServerJMS not responding.");
            }
            ConnectOkMessage connectOk = (ConnectOkMessage) connectReply.getObject();
            if (connectOk.getChannels().isEmpty()) {
                throw new Exception("Server denied connection. Possible nick duplicate.");
            }
            if (!connectOk.getChannels().contains(myChannelName)) {
                throw new Exception("Channel does not exist: " + myChannelName);
            }

            myQueue = (Queue) ic.lookup(USER_QUEUE + nick);

            // Asynchronous listener on private user queue to auto-reply to direct messages
            JMSContext privateCtx = cfac.createContext();
            JMSProducer privateProducer = privateCtx.createProducer();
            JMSConsumer privateConsumer = privateCtx.createConsumer(myQueue);

            privateConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message msg) {
                    try {
                        AMessage bmsg = (AMessage) ((ObjectMessage) msg).getObject();
                        if (bmsg.getType() == AMessage.ChatMessageType.CHAT_MESSAGE) {
                            ChatMessage chat = (ChatMessage) bmsg;
                            System.out.println("Private message from " + chat.getSource() + ": " + chat.getLine());
                            if (!chat.getSource().equals(nick)) {
                                Queue replyQueue = (Queue) ic.lookup(USER_QUEUE + chat.getSource());
                                privateProducer.send(replyQueue,
                                    privateCtx.createObjectMessage(MessageFactory.chatMessage(nick, "I am an automated bot.")));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception processing private message: " + e);
                    }
                }
            });

            // Join target channel topic
            Message joinMsg = ctx.createObjectMessage(MessageFactory.joinMessage(myChannelName, nick));
            joinMsg.setJMSReplyTo(tempQueue);
            producer.send(serverQueue, joinMsg);

            ObjectMessage joinReply = (ObjectMessage) consumer.receive(2000);
            if (joinReply == null) {
                throw new Exception("ChatServerJMS did not confirm join to channel " + myChannelName);
            }
            JoinOkMessage joinOk = (JoinOkMessage) joinReply.getObject();
            System.out.println("Joined channel " + joinOk.getChannelName() + " with active users: " + joinOk.getUsers());

            channelTopic = (Topic) ic.lookup(CHANNEL_TOPIC + myChannelName);

            // Announce presence in channel
            producer.send(channelTopic,
                ctx.createObjectMessage(MessageFactory.chatMessage(nick,
                    "Hello, I am " + nick + " and I joined channel #" + myChannelName)));

            // Message processing loop for channel events
            consumer.close();
            consumer = ctx.createConsumer(channelTopic);

            while (true) {
                ObjectMessage msg = (ObjectMessage) consumer.receive();
                AMessage bmsg = (AMessage) msg.getObject();

                switch (bmsg.getType()) {
                    case USER_JOINS: {
                        UserJoinsMessage jm = (UserJoinsMessage) bmsg;
                        if (!jm.getNick().equals(nick)) {
                            String text = "Welcome, " + jm.getNick() + "! (automated bot)";
                            Queue userQueue = (Queue) ic.lookup(USER_QUEUE + jm.getNick());
                            producer.send(channelTopic, ctx.createObjectMessage(MessageFactory.chatMessage(nick, text)));
                            producer.send(userQueue, ctx.createObjectMessage(MessageFactory.chatMessage(nick, text)));
                        }
                        break;
                    }
                    case USER_LEAVES: {
                        UserLeavesMessage lm = (UserLeavesMessage) bmsg;
                        if (!lm.getNick().equals(nick)) {
                            String text = "Goodbye " + lm.getNick() + ", see you soon!";
                            Queue userQueue = (Queue) ic.lookup(USER_QUEUE + lm.getNick());
                            producer.send(channelTopic, ctx.createObjectMessage(MessageFactory.chatMessage(nick, text)));
                            producer.send(userQueue, ctx.createObjectMessage(MessageFactory.chatMessage(nick, text)));
                        }
                        break;
                    }
                    case CHAT_MESSAGE: {
                        ChatMessage cm = (ChatMessage) bmsg;
                        System.out.println("Channel message from " + cm.getSource() + ": " + cm.getLine());
                        break;
                    }
                    default:
                        System.out.println("Unhandled event type: " + bmsg.getType());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in ChatRobot: " + e);
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }
}