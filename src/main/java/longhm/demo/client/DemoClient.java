package longhm.demo.client;

import longhm.demo.thrift.CalcService;
import longhm.thrift.client.auto_reconnect.AutoReconnectClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class DemoClient {
    public static void main(String[] args) throws TTransportException {
        try {
            CalcService.Iface client = AutoReconnectClient.getClient(
                    CalcService.Iface.class,
                    "localhost",
                    9000,
                    (host, port) -> new TFramedTransport(new TSocket(host, port)),
                    new CalcService.Client.Factory()
            );
            runClient(client);
        } catch (TException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void runClient(CalcService.Iface client) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            System.out.println("Running ");
            try {
                Integer add = client.add(1, 2);
                System.out.println(add);
            } catch (TException e) {
                System.out.println(e);
            }
        }
    }
}
