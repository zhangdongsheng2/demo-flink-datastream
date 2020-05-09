package com.commerce.commons.model;

import lombok.Data;
import org.apache.kafka.common.internals.Topic;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 11:18
 */
@Data
public class IotDevice {
    private String id;

    private String devName;
    //dev_seri_no
    private String serialNumber;
    private String secret;
    private String noteName;
    private String ipAddr;
    private String firmwareVer;
    private String configFileVer;
    private LocalDateTime activeTime;
    private LocalDateTime keepOnlineOrOfflineTime;
    private LocalDateTime lastOnlineTime;
    //DeviceStatus
    private String status;
    //DeviceStatus
    private String activeStatus;
    //AdjustStatus
    private String adjustStatus;
    //FirmwareUpdateStatus
    private String firmwareUpdateStatus;
    private LocalDateTime statusUpdateTime;
    private Byte isDel = 0;
    private String domain;
    private String mqttServer;
    private Set<Topic> topicSet;
}
