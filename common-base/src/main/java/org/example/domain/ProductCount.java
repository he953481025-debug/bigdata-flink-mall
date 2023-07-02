package org.example.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author hejincai
 * @since 2023/7/2 15:53
 **/
@Data
@EqualsAndHashCode(of = "itemId")
public class ProductCount {

    /**
     * 商品id
     */
    private String itemId;

    /**
     * 数量
     */
    private Integer count;

}
