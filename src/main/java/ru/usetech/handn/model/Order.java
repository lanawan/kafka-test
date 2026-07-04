package ru.usetech.handn.model;

import lombok.*;
import java.math.BigDecimal;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString // Полезно для логирования в Allure или консоль при падении тестов
public class Order {
    private String orderId;
    private String status;
    private BigDecimal amount;
}
