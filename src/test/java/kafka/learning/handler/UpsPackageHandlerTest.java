package kafka.learning.handler;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import kafka.learning.model.UpsPackage;
import kafka.learning.service.UpsPackageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UpsPackageHandlerTest {

  private UpsPackageHandler handler;
  private UpsPackageService upsPackageServiceMock;

  //fali servis
  @BeforeEach
  void setUp() {
    upsPackageServiceMock = mock(UpsPackageService.class);
    handler = new UpsPackageHandler(upsPackageServiceMock);
  }

  @Test
  void listen_Success() throws Exception {
    UpsPackage testEvent = UpsPackage.builder().height(5).width(7).length(2).build();
    handler.listen(testEvent);
    verify(upsPackageServiceMock, times(1)).process(testEvent);
  }

  @Test
  void listen_ServiceThrowsException() throws Exception {
    UpsPackage testEvent = UpsPackage.builder().height(5).width(7).length(2).build();
    doThrow(new RuntimeException("Service failure")).when(upsPackageServiceMock).process(testEvent);

    handler.listen(testEvent);

    verify(upsPackageServiceMock, times(1)).process(testEvent);
  }

}