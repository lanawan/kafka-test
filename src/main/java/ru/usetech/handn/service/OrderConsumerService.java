package ru.usetech.handn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.usetech.handn.model.Order;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class OrderConsumerService {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private final String bootstrapServers;
    private final String groupId;
    private final String topicName;
    private final Consumer<Order> orderProcessor;

    // Поля для поддержки Dead Letter Queue (DLQ)
    private final String dlqTopicName;
    private final KafkaProducer<String, String> dlqProducer;

    private final Counter successCounter;
    private final Counter failureCounter;

    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private volatile boolean running = false;

    // Полный конструктор с поддержкой DLQ
    public OrderConsumerService(String bootstrapServers, String groupId, String topicName,
                                String dlqTopicName, KafkaProducer<String, String> dlqProducer,
                                MeterRegistry meterRegistry, Consumer<Order> orderProcessor) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topicName = topicName;
        this.dlqTopicName = dlqTopicName;
        this.dlqProducer = dlqProducer;
        this.orderProcessor = orderProcessor;

        this.successCounter = Counter.builder("kafka.consumer.orders.processed")
                .tag("group", groupId).tag("topic", topicName).tag("status", "success").register(meterRegistry);
        this.failureCounter = Counter.builder("kafka.consumer.orders.processed")
                .tag("group", groupId).tag("topic", topicName).tag("status", "failure").register(meterRegistry);
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);

        consumerThread = new Thread(() -> {
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            // Если тут падает Jackson — управление перехватывает catch блок ниже
                            Order order = objectMapper.readValue(record.value(), Order.class);
                            orderProcessor.accept(order);
                            successCounter.increment();
                        } catch (Exception e) {
                            failureCounter.increment();
                            log.error("[{}] Ошибка парсинга JSON или обработки сообщения. Отправляем в DLQ: {}", groupId, e.getMessage());

                            // Автоматическая отправка сырых «битых» данных в DLQ топик
                            if (dlqProducer != null && dlqTopicName != null) {
                                try {
                                    // get() для синхронного ожидания доставки в DLQ (гарантия доставки ошибки)
                                    dlqProducer.send(new ProducerRecord<>(dlqTopicName, record.key(), record.value())).get();
                                    log.info("Сообщение успешно перенаправлено в DLQ: {}", dlqTopicName);
                                } catch (Exception dlqException) {
                                    log.error("Критическая ошибка: Не удалось отправить сообщение в DLQ!", dlqException);
                                    // Тут в проде обычно останавливают контейнер или кидают RuntimeException
                                }
                            }
                        }
                    }
                }
            } catch (WakeupException e) {
                log.info("[{}] Сервис остановлен через wakeup.", groupId);
            } finally {
                consumer.close();
            }
        }, "kafka-consumer-" + groupId);

        consumerThread.start();
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        if (consumer != null) consumer.wakeup();
        try {
            if (consumerThread != null) consumerThread.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

