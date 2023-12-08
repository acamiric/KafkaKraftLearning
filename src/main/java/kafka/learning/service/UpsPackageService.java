package kafka.learning.service;

import kafka.learning.model.UpsPackage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class UpsPackageService {

  private static final String SMALL_TRUCK = "small.truck";
  private static final String LARGE_TRUCK = "large.truck";

  private final KafkaTemplate<String, Object> kafkaProducer;

  public void process(UpsPackage sentPackage) throws Exception {
    log.info("Calculating the package volume");
    int packageVolume = sentPackage.getHeight()*sentPackage.getWidth()*sentPackage.getLength();
    log.info("Package volume is: " + packageVolume);

    if(packageVolume<=50) {
      log.info("Package sent via Small truck");
      kafkaProducer.send(SMALL_TRUCK, sentPackage).get();
    }
    else {
      log.info("Package sent via Large truck");
      kafkaProducer.send(LARGE_TRUCK, sentPackage).get();
    }
  }

}
