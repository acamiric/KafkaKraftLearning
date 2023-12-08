package kafka.learning.handler;

import kafka.learning.model.UpsPackage;
import kafka.learning.service.UpsPackageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class UpsPackageHandler {

  private final UpsPackageService service;


  @KafkaListener(
      id = "firstPoint",
      topics = "package.for.delivery",
      groupId = "package.ups"
  )
  public void listen(UpsPackage payload) throws Exception {
    log.info("Package arrived to aaa topic and is being processed "+payload);
    try {
      service.process(payload);
    } catch (Exception e) {
      log.error("Processing failure", e);
    }
  }

}
