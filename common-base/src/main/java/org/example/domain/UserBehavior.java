package org.example.domain;

import lombok.Data;

/**
 * @author hejincai
 * @since 2023/7/2 10:58
 **/
@Data
public class UserBehavior {

    /**
     * 用户id
     */
    private String userId;

    /**
     * 商品id
     */
    private String itemId;

    /**
     * 商品分类id
     */
    private String categoryId;

    /**
     * 用户行为类型
     */
    private String behavior;

    /**
     * 时间戳
     */
    private Long timestamp;


}
