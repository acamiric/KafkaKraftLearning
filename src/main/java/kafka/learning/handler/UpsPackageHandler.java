package kafka.learning.handler;

import kafka.learning.model.UpsMail;
import kafka.learning.model.UpsPackage;
import kafka.learning.service.UpsPackageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
@KafkaListener(
    id = "firstPoint",
    topics = "package.for.delivery",
    groupId = "package.ups"
)
public class UpsPackageHandler {

  private final UpsPackageService service;


  @KafkaHandler
  public void listen(UpsPackage payload) throws Exception {
    log.info("Package arrived to package.for.delivery topic and is being processed " + payload);
    try {
      service.process(payload);
    } catch (Exception e) {
      log.error("Processing failure", e);
    }
  }

  @KafkaHandler
  public void listen(UpsMail payload) throws Exception {
    log.info("Mail arrived to package.for.delivery topic and is being processed " + payload);
    try {
      service.process(payload);
    } catch (Exception e) {
      log.error("Processing failure", e);
    }
  }

}
