package data.mgmt.cdckafka.service;

import java.util.List;
public interface CDCListenerService {

    String cdcStart(Integer taskId, boolean isInitial);

    String cdcStop(Integer taskId);

    String cdcStatus(Integer taskId);



}
