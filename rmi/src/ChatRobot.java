import utils_rmi.ChatConfiguration;
import faces.IChatChannel;
import faces.IChatMessage;
import faces.IChatServer;
import faces.IChatUser;
import faces.INameServer;
import faces.MessageListener;
import impl.ChatMessageImpl;
import impl.ChatUserImpl;

/**
 * Headless automated bot client built with Java RMI.
 * 
 * Connects to the remote chat server via the naming registry, joins a designated
 * channel, listens for messages, and automatically responds to channel greetings.
 */
public class ChatRobot implements MessageListener {

    private final ChatConfiguration conf;
    private IChatServer srv;
    private IChatUser myUser;
    private IChatChannel channel;

    public ChatRobot(ChatConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public void messageArrived(IChatMessage msg) {
        try {
            IChatUser src = msg.getSender();
            String senderNick = (src != null) ? src.getNick() : "Unknown";
            String text = msg.getText();
            System.out.println("Message received from [" + senderNick + "]: " + text);

            if (myUser != null && !senderNick.equals(myUser.getNick())) {
                if (channel != null) {
                    IChatMessage reply = new ChatMessageImpl(myUser, channel, "Welcome, " + senderNick + "! (automated bot)");
                    channel.sendMessage(reply);
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing arrived message: " + e.getMessage());
        }
    }

    private void work() {
        String channelName = conf.getChannelName();
        if (channelName == null) channelName = "#Linux";
        String nick = conf.getNick();
        if (nick == null) nick = "chat-bot";

        System.out.println("ChatRobot RMI starting: connecting to '" + conf.getServerName() +
                "', channel: '" + channelName + "', nick: '" + nick + "'");

        try {
            INameServer ns = INameServer.getNameServer(conf.getNameServerHost(), conf.getNameServerPort());
            srv = (IChatServer) ns.lookup(conf.getServerName());
            if (srv == null) {
                System.err.println("ChatServer not found: " + conf.getServerName());
                return;
            }

            myUser = new ChatUserImpl(nick, this);
            srv.connectUser(myUser);

            channel = srv.getChannel(channelName);
            if (channel == null) {
                System.err.println("Channel not found: " + channelName);
                return;
            }

            channel.join(myUser);
            channel.sendMessage(new ChatMessageImpl(myUser, channel, "Hello, I am " + nick + " and I joined channel " + channelName));
            System.out.println("ChatRobot RMI connected and listening on " + channelName);

            // Keep process active
            Object lock = new Object();
            synchronized (lock) {
                lock.wait();
            }
        } catch (Exception e) {
            System.err.println("ChatRobot error: " + e);
        }
    }

    public static void main(String[] args) {
        ChatRobot cr = new ChatRobot(ChatConfiguration.parse(args));
        cr.work();
    }
}
