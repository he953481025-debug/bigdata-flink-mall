package org.example.domain;

import lombok.Data;

/**
 * @author hejincai
 * @since 2023/7/2 20:32
 **/
@Data
public class OrderEvent {
    /**
     * 订单id
     */
    private String orderId;

    /**
     * 订单状态 1=待支付，2=超时支付，3=已支付
     */
    private String orderStatus;

    /**
     * 用户id
     */
    private String userId;

    /**
     * 订单id
     */
    private String productId;

    /**
     * 时间戳
     */
    private Long eventTime;
}
