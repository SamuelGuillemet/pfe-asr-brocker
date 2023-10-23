package fix.server;
import quickfix.*;


public class Main {
    public static void main(String[] args) throws ConfigError {
        try {
            SessionSettings settings = new SessionSettings("config/server.cfg");

            Application application = new Application();

            MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new FileLogFactory(settings);

            MessageFactory messageFactory = new DefaultMessageFactory();
            Acceptor acceptor = new SocketAcceptor(application, messageStoreFactory, settings, logFactory, messageFactory);
            System.out.println("Starting server...");
            acceptor.start();
            System.out.println("FIX server is running...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
