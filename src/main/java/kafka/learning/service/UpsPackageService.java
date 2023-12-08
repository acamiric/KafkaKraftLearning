package kafka.learning.service;

import kafka.learning.model.UpsMail;
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
  private static final String MAIL_BOX = "mail.box";

  private final KafkaTemplate<String, Object> kafkaProducer;

  public void process(UpsPackage sentPackage) throws Exception {
    log.info("Calculating the package volume");
    int packageVolume = sentPackage.getHeight()*sentPackage.getWidth()*sentPackage.getLength();
    log.info("Package volume is: " + packageVolume);

    if(packageVolume<=50) {
      kafkaProducer.send(SMALL_TRUCK, sentPackage).get();
      log.info("Package sent via Small truck");
    }
    else {
      kafkaProducer.send(LARGE_TRUCK, sentPackage).get();
      log.info("Package sent via Large truck");
    }
  }


  public void process(UpsMail sentMail) throws Exception {
    log.info("Sending mail to mail box");
      kafkaProducer.send(MAIL_BOX, sentMail).get();
    log.info("Mail sent to mail box {}", sentMail);

  }

}
