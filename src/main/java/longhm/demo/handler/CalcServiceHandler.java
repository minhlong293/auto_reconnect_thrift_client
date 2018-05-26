package longhm.demo.handler;

import longhm.demo.thrift.CalcService;
import org.apache.thrift.TException;

public class CalcServiceHandler implements CalcService.Iface {
    private int count = 0;

    @Override
    public String sayHello() throws TException {
        return "Hello World! " + count;
    }

    @Override
    public int add(int a, int b) throws TException {
        count++;
        return a + b;
    }
}
