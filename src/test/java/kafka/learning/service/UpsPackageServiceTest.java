package kafka.learning.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import kafka.learning.model.UpsPackage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class UpsPackageServiceTest {

  private KafkaTemplate kafkaProducerMock;

  private UpsPackageService service;

  @BeforeEach
  void setUp() {
    kafkaProducerMock = mock(KafkaTemplate.class);
    service = new UpsPackageService(kafkaProducerMock);
  }

  @Test
  void process_sendToLargeTruck_Success() throws Exception {
    when(kafkaProducerMock.send(anyString(), any(UpsPackage.class))).thenReturn(
        mock(CompletableFuture.class));

    UpsPackage testEvent = UpsPackage.builder().height(5).width(7).length(2).build();
    service.process(testEvent);

    verify(kafkaProducerMock, times(1)).send(eq("large.truck"), any(UpsPackage.class));
  }

  @Test
  void process_sendToSmallTruck_Success() throws Exception {
    when(kafkaProducerMock.send(anyString(), any(UpsPackage.class))).thenReturn(
        mock(CompletableFuture.class));

    UpsPackage testEvent = UpsPackage.builder().height(2).width(7).length(3).build();
    service.process(testEvent);

    verify(kafkaProducerMock, times(1)).send(eq("small.truck"), any(UpsPackage.class));
  }

  @Test
  void process_ProducerThrowsException() {
    UpsPackage testEvent = UpsPackage.builder().height(2).width(7).length(3).build();
    doThrow(new RuntimeException("Producer failure")).when(kafkaProducerMock)
        .send(eq("small.truck"), any(UpsPackage.class));

    Exception exception = assertThrows(RuntimeException.class, () -> service.process(testEvent));

    verify(kafkaProducerMock, times(1)).send(eq("small.truck"), any(UpsPackage.class));
    assertThat(exception.getMessage(), equalTo("Producer failure"));
  }

}