
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.usetech.handn.model.Order;
import ru.usetech.handn.service.OrderConsumerService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Slf4j
public class KafkaIntegrationTest {
    private static final String DLQ_TOPIC_NAME = "test-orders-dlq";
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private SimpleMeterRegistry meterRegistry;

    @Container
    private static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    private static String bootstrapServers;

    @BeforeAll
    static void init() {
        bootstrapServers = kafkaContainer.getBootstrapServers();
    }

    @BeforeEach
    void setUp() {
        this.meterRegistry = new SimpleMeterRegistry();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private Properties getRawConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-verifier-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Test
    @Feature("Обработка заказов через Kafka")
    @Story("Успешное получение валидного заказа")
    @Description("Тест проверяет, что валидный JSON успешно десериализуется, попадает в обработчик и инкрементирует счетчик успехов в Micrometer")
    void testSendAndReceiveMessageWithMetrics() throws Exception {
        Order testOrder = new Order("order-123", "CREATED", new BigDecimal("1500.0"));
        CopyOnWriteArrayList<Order> receivedOrders = new CopyOnWriteArrayList<>();

        String topicForTest = "orders-success-topic";
        OrderConsumerService service = new OrderConsumerService(
                bootstrapServers, "test-group", topicForTest, null, null, meterRegistry, receivedOrders::add
        );

        service.start();

        try (KafkaProducer<String, String> producer = createProducer()) {
            String jsonPayload = objectMapper.writeValueAsString(testOrder);
            producer.send(new ProducerRecord<>(topicForTest, testOrder.getOrderId(), jsonPayload));

            // Ждем обработку без Thread.sleep
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(receivedOrders).isNotEmpty();
                assertThat(receivedOrders.get(0).getOrderId()).isEqualTo("order-123");

                double successCount = meterRegistry.get("kafka.consumer.orders.processed")
                        .tag("status", "success").counter().count();
                log.info(">>>> ФАКТИЧЕСКОЕ КОЛИЧЕСТВО МЕТРИК В ТЕСТЕ: {} <<<<", successCount);
                assertThat(successCount).isEqualTo(1.0);
            });
        } finally {
            service.stop();
        }
    }

    @Test
    @Feature("Обработка заказов через Kafka")
    @Story("Обработка поврежденных сообщений (DLQ)")
    @Description("Тест проверяет, что битый JSON не ломает консьюмер, увеличивает счетчик ошибок и перенаправляется в DLQ-топик")
    void testInvalidJsonSentToDlq() {
        String invalidJson = "{ \"orderId\": \"order-999\", \"status\": \"FAILED\", \"amount\": \"not-a-number\" }";
        CopyOnWriteArrayList<Order> receivedOrders = new CopyOnWriteArrayList<>();
        String topicForDlqTest = "orders-dlq-topic";
        try (KafkaProducer<String, String> dlqProducer = createProducer()) {
            OrderConsumerService service = new OrderConsumerService(
                    bootstrapServers, "test-group-dlq", topicForDlqTest, DLQ_TOPIC_NAME, dlqProducer, meterRegistry, receivedOrders::add
            );
            service.start();

            // Отправляем некорректный JSON в основной топик
            dlqProducer.send(new ProducerRecord<>(topicForDlqTest, "corrupt-key", invalidJson));

            // Проверяем, что в базу ничего не попало, а счетчик ошибок вырос
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                double failureCount = meterRegistry.get("kafka.consumer.orders.processed")
                        .tag("status", "failure").counter().count();
                assertThat(failureCount).isEqualTo(1.0);
                assertThat(receivedOrders).isEmpty();
            });

            // Вычитываем из DLQ топика и проверяем, что битые данные дошли туда
            Properties consumerProps = getRawConsumerProps();
            try (KafkaConsumer<String, String> dlqConsumer = new KafkaConsumer<>(consumerProps)) {
                dlqConsumer.subscribe(Collections.singletonList(DLQ_TOPIC_NAME));

                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                    ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.ofMillis(200));
                    assertThat(records).isNotEmpty();
                    ConsumerRecord<String, String> record = records.iterator().next();
                    assertThat(record.key()).isEqualTo("corrupt-key");
                    assertThat(record.value()).contains("not-a-number");
                });
            } finally {
                service.stop();
            }
        }
    }
}