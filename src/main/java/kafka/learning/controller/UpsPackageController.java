package kafka.learning.controller;

import kafka.learning.model.UpsMail;
import kafka.learning.model.UpsPackage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ups")
@RequiredArgsConstructor
@Slf4j
public class UpsPackageController {

  private final KafkaTemplate<String,Object> kafkaTemplate;
  private static final String PACKAGE_FOR_DELIVERY="package.for.delivery";

  @PostMapping("/package")
  public ResponseEntity<UpsPackage> sendPackageToUps(
      @RequestBody UpsPackage upsPackage) throws Exception {
    log.info("Sending package to UPS: {}",upsPackage);
    kafkaTemplate.send(PACKAGE_FOR_DELIVERY, upsPackage);
    return ResponseEntity.status(HttpStatus.CREATED).body(upsPackage);
  }

  @PostMapping("/mail")
  public ResponseEntity<UpsMail> sendMailToUps(
      @RequestBody UpsMail upsMail) throws Exception {
    log.info("Sending mail to UPS: {}",upsMail);
    kafkaTemplate.send(PACKAGE_FOR_DELIVERY, upsMail);
    return ResponseEntity.status(HttpStatus.CREATED).body(upsMail);
  }

}
