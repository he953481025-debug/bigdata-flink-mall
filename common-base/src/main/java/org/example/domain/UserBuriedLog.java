package org.example.domain;

import lombok.Data;

/**
 * @author hejincai
 * @since 2023/7/2 11:16
 **/
@Data
public class UserBuriedLog {

    private String ip;

    private String userId;

    private Long eventTime;

    private String method;

    private String url;
}
