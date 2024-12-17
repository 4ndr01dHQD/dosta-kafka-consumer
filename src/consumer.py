import json
from typing import Any

from confluent_kafka import Consumer

from manager import HeadersManager


class KafkaConsumer:

    def __init__(
            self,
            config: dict,
            topics: tuple[str],
            headers_manager: HeadersManager,
            logger: Any,
    ):
        self.consumer = Consumer(config)
        self.logger = logger
        self._manager = headers_manager
        self._topics = topics

    def subscribe(self):
        try:
            self.consumer.subscribe(self._topics)
            self.logger.info(
                'Консьюмер создан и подписан на топики',
                topics=self._topics,
            )
        except Exception as e:
            self.logger.error(
                f'Ошибка при попытке подписаться на топики: {e=}',
                topics=self._topics,
            )
            self.consumer.close()
            raise

    def run(self):
        while True:
            try:
                message = self.consumer.poll(timeout=1.0)

                if message is None:
                    continue

                offset = message.offset()

                if message.error():
                    self.logger.error(
                        f'Ошибка во время получения сообщения',
                        message_offset=offset,
                        error=message.error(),
                    )
                    continue

                self.logger.info(
                    f'Получено сообщение {message.key()=}',
                    message_offset=offset,
                )
                key = message.key().decode('utf-8') if message.key() else ""
                value = json.loads(message.value())
                handler = self._manager.get_handler_cls(message)
                data = handler.validate_data(value)
                result = handler.handle(key, data)

            except Exception as message_error:
                self.logger.exception(
                    f'Ошибка при обработке сообщения: {message_error=}',
                    exc_info=True,
                    offset=offset,
                    key=key,
                )
                continue

            else:
                self.logger.info('Сообщение обработано', offset=offset, result=result)
                self.consumer.commit(message)
