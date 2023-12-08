package kafka.learning.controller;

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
@RequestMapping("/paket")
@RequiredArgsConstructor
@Slf4j
public class UpsPackageController {

  private final KafkaTemplate<String,Object> kafkaTemplate;
  private static final String PACKAGE_FOR_DELIVERY="package.for.delivery";

  @PostMapping
  public ResponseEntity<UpsPackage> sendPackageToUps(
      @RequestBody UpsPackage upsPackage) throws Exception {
    log.info("Sending package to UPS: {}",upsPackage);
    kafkaTemplate.send(PACKAGE_FOR_DELIVERY, upsPackage);
    return ResponseEntity.status(HttpStatus.CREATED).body(upsPackage);
  }

}
