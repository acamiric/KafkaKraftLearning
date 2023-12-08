package kafka.learning.integration;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.learning.config.UpsPackageConfig;
import kafka.learning.model.UpsMail;
import kafka.learning.model.UpsPackage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = UpsPackageConfig.class)
@ActiveProfiles("test")
//@EmbeddedKafka(controlledShutdown = true)
@EmbeddedKafka(controlledShutdown = true, topics = {"small.truck", "large.truck",
    "package.for.delivery", "mail.box"})
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@Slf4j
class UpsPackageIntegrationTest {

  private static final String PACKAGE_FOR_DELIVERY = "package.for.delivery";
  private static final String SMALL_TRUCK = "small.truck";
  private static final String LARGE_TRUCK = "large.truck";
  private static final String MAIL_BOX = "mail.box";


  @Autowired
  private KafkaTemplate kafkaTemplate;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  private KafkaListenerEndpointRegistry registry;

  @Autowired
  private KafkaTestListener testListener;

  @Configuration
  static class TestConfig {

    @Bean
    public KafkaTestListener testListener() {
      return new KafkaTestListener();
    }


  }

  public static class KafkaTestListener {

    AtomicInteger smallTruckCounter = new AtomicInteger(0);
    AtomicInteger largeTruckCounter = new AtomicInteger(0);
    AtomicInteger mailBoxCounter = new AtomicInteger(0);

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = SMALL_TRUCK)
    void receiveSmallTruck(@Payload UpsPackage payload) {
      log.debug("Received UpsPackage: {}", payload);
      smallTruckCounter.incrementAndGet();

    }

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = LARGE_TRUCK)
    void receiveLargeTruck(@Payload UpsPackage payload) {
      log.debug("Received UpsPackage: {}", payload);
      largeTruckCounter.incrementAndGet();

    }

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = MAIL_BOX)
    void receiveMail(@Payload UpsMail payload) {
      log.debug("Received UpsMail: {}", payload);
      mailBoxCounter.incrementAndGet();

    }

  }

  @BeforeEach
  public void setUp() {
    testListener.largeTruckCounter.set(0);
    testListener.smallTruckCounter.set(0);
    testListener.mailBoxCounter.set(0);

    registry.getListenerContainers().stream().forEach(container ->
        ContainerTestUtils.waitForAssignment(container,
            embeddedKafkaBroker.getPartitionsPerTopic()));


  }

  @Test
  void testUpsPackageSmallTruckFlow() throws Exception {
    UpsPackage testEvent = UpsPackage.builder().height(5).width(4).length(2).build();
    sendMessage(SMALL_TRUCK, testEvent);
    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
        .until(testListener.smallTruckCounter::get, equalTo(1));


  }

  @Test
  void testUpsPackageLargeTruckFlow() throws Exception {
    UpsPackage testEvent = UpsPackage.builder().height(5).width(7).length(20).build();
    sendMessage(LARGE_TRUCK, testEvent);
    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
        .until(testListener.largeTruckCounter::get, equalTo(1));


  }

  @Test
  void testMailFlow() throws Exception {
    UpsMail testEvent = UpsMail.builder().mail("Hello from mail success test").build();
    sendMessage(MAIL_BOX, testEvent);
    await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
        .until(testListener.mailBoxCounter::get, equalTo(1));


  }

  private void sendMessage(String topic, Object data) throws Exception {
    kafkaTemplate.send(
        MessageBuilder
            .withPayload(data)
            .setHeader(KafkaHeaders.TOPIC, topic)
            .build()).get();
  }

}
