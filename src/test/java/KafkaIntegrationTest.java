import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class KafkaIntegrationTest {

    private static final String TOPIC_NAME = "test-orders-topic";
    // Имитируем базу данных нашей системы
    private final ConcurrentHashMap<String, String> ordersDatabase = new ConcurrentHashMap<>();
    // Счётчик реальных списаний денег / обработок
    private final AtomicInteger processingCounter = new AtomicInteger(0);

    // Автоматически поднимаем контейнер с Kafka перед тестами
    @Container
    private static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    private static String bootstrapServers;

    @BeforeAll
    static void init() {
        // Получаем динамический порт, который Testcontainers выделил для Kafka
        bootstrapServers = kafkaContainer.getBootstrapServers();

        // Явно создаем топик перед запуском тестов
        // с целью исключения что Consumer запустится раньше, чем Producer в условиях асинхронности и будет пытаться
        // подписаться (poll()) на еще несуществующий топик
        // но это опционально, т.к. через милисекунды Consumer всё равно найдет новый топик сделанный Producer
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1); // имя, partitions, replication factor
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Топик " + TOPIC_NAME + " успешно создан вручную.");
        } catch (Exception e) {
            throw new RuntimeException("Не удалось создать топик Kafka", e);
        }
    }

    @Test
    void testSendAndReceiveMessage() {
        String expectedKey = "order-123";
        String expectedValue = "{\"status\":\"CREATED\", \"amount\":1500}";

        // 1. Настраиваем и создаем Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // 2. Настраиваем и создаем Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // Поток-безопасный список для сохранения полученных сообщений
        CopyOnWriteArrayList<ConsumerRecord<String, String>> receivedRecords = new CopyOnWriteArrayList<>();

        // Запускаем чтение в отдельном потоке (имитируем работу тестируемого микросервиса)
        Thread consumerThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(receivedRecords::add);
                }
            } catch (org.apache.kafka.common.errors.InterruptException |
                     org.apache.kafka.common.errors.WakeupException e) {
                // Мягко ловим прерывание, чтобы не спамить в консоль ошибками
                System.out.println("Консьюмер успешно остановлен.");
            }
        });
        consumerThread.start();

        try {
            // 3. Действие: Отправляем сообщение в Kafka
            producer.send(new ProducerRecord<>(TOPIC_NAME, expectedKey, expectedValue));

            // 4. Проверка (Assert): Ждем появления сообщения с помощью Awaitility
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(receivedRecords).isNotEmpty();
                ConsumerRecord<String, String> record = receivedRecords.get(0);
                assertThat(record.key()).isEqualTo(expectedKey);
                assertThat(record.value()).isEqualTo(expectedValue);
            });

        } finally {
            // Очищаем ресурсы
            consumerThread.interrupt();
            producer.close();
            consumer.close();
        }
    }

    @Test
    void testConsumerIdempotency() {
        String orderId = "order-777";
        String orderPayload = "{\"amount\":5000}";

        // 1. НАСТРОЙКА: Имитируем логику микросервиса (Consumer с проверкой дубликатов)
        // На собеседовании этот код будет сидеть внутри тестируемого приложения!
        Thread businessServiceConsumer = new Thread(() -> {
            // Настраиваем KafkaConsumer (как в прошлом шаге)
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotent-service-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String currentOrderId = record.key();

                        // КЛЮЧЕВАЯ ПРОВЕРКА НА ИДЕМПОТЕНТНОСТЬ (Бизнес-логика)
                        if (ordersDatabase.containsKey(currentOrderId)) {
                            System.out.println("⚠ [КОНСЬЮМЕР] Обнаружен дубликат заказа: " + currentOrderId + ". Игнорируем.");
                        } else {
                            // Первая обработка заказа
                            ordersDatabase.put(currentOrderId, record.value());
                            processingCounter.incrementAndGet(); // увеличиваем счётчик списаний
                            System.out.println("✅ [КОНСЬЮМЕР] Заказ успешно обработан: " + currentOrderId);
                        }
                    }
                }
            } finally {
                consumer.close();
            }
        });
        businessServiceConsumer.start();

        // 2. ДЕЙСТВИЕ: Создаем продюсер и отправляем ДВА одинаковых сообщения
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            // Отправляем первый раз
            producer.send(new ProducerRecord<>(TOPIC_NAME, orderId, orderPayload));

            // Отправляем точно такой же заказ второй раз (имитируем сетевой сбой в Kafka)
            producer.send(new ProducerRecord<>(TOPIC_NAME, orderId, orderPayload));
        }

        // 3. ПРОВЕРКА (ASSERT): Ждем завершения обработки
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            // Проверяем, что в БД запись появилась
            assertThat(ordersDatabase).containsKey(orderId);

            // ГЛАВНЫЙ АССЕРТ: счетчик обработок должен быть равен 1, несмотря на то, что сообщений было 2!
            assertThat(processingCounter.get())
                    .as("Бизнес-логика выполнилась строго один раз!")
                    .isEqualTo(1);
        });

        businessServiceConsumer.interrupt();
    }

    @Test
    void testDeadLetterQueueOnInvalidJson() {
        String invalidPayload = "NOT_A_JSON_STRING";
        String dlqTopicName = TOPIC_NAME + ".DLQ";

        // Поток-безопасный список для сообщений, которые улетят в DLQ
        CopyOnWriteArrayList<String> dlqRecords = new CopyOnWriteArrayList<>();

        // 1. НАСТРОЙКА: Имитируем сервис, который при ошибке пересылает сообщение в DLQ
        Thread mainServiceConsumer = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "main-service-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            Properties prodProps = new Properties();
            prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                 KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(prodProps)) {

                consumer.subscribe(Collections.singletonList(TOPIC_NAME));

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            // Имитируем парсинг JSON. Если строка не начинается с '{', бросаем ошибку
                            if (!record.value().startsWith("{")) {
                                throw new IllegalArgumentException("Критическая ошибка: Невалидный JSON формат!");
                            }
                            System.out.println("Парсинг успешен: " + record.value());
                        } catch (Exception e) {
                            System.out.println("⚠ [СЕРВИС] Ошибка обработки! Отправляем сообщение в DLQ...");
                            // Пересылаем «битое» сообщение в топик плохих ответов
                            dlqProducer.send(new ProducerRecord<>(dlqTopicName, record.key(), record.value()));
                        }
                    }
                }
            }
        });
        mainServiceConsumer.start();

        // 2. НАСТРОЙКА: Консьюмер автотеста, который просто следит за топиком DLQ
        Thread dlqTestConsumer = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-assert-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(dlqTopicName));
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(r -> dlqRecords.add(r.value()));
                }
            }
        });
        dlqTestConsumer.start();

        // 3. ДЕЙСТВИЕ: Отправляем битые данные в основной топик
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "bad-key", invalidPayload));
        }

        // 4. ПРОВЕРКА (ASSERT): Проверяем, что сообщение долетело до DLQ топика
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(dlqRecords)
                    .as("Битое сообщение должно быть перенаправлено в топик DLQ")
                    .contains(invalidPayload);
        });

        mainServiceConsumer.interrupt();
        dlqTestConsumer.interrupt();
    }
}
