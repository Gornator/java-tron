package org.tron.plugins.ethfreeze;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import picocli.CommandLine;

@Slf4j(topic = "DB")
@CommandLine.Command(name = "test",
    description = "test any function")
public class Test implements Callable<Integer> {

  public void testZmqSub() {
    ZContext context = new ZContext();
    ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
    subscriber.connect(String.format("tcp://localhost:%d", 50096));
    subscriber.subscribe("blockTrigger");

    while (!Thread.currentThread().isInterrupted()) {
      byte[] message = subscriber.recv();
      String triggerMsg = new String(message);
      logger.info("Receive msg: {}", triggerMsg);
    }
  }

  @Override
  public Integer call() throws Exception {
    testZmqSub();
    return 0;
  }
}
