from confluent_kafka import Message

from handlers.base import AbstractHandler

from exceptions import HeadersError, HandlerError


class HeadersManager:

    headers_mapping = None
    _HEADER_KEY = 'content-type'

    def __init__(self, headers_mapping: dict):
        self._handlers = {}

        if self.headers_mapping is not None:
            for header, handler_cls in self.headers_mapping.items():
                self._register_handler(header, handler_cls)

    def _register_handler(self, name: str, handler_instance: type[AbstractHandler]):
        self._handlers[name] = handler_instance

    def get_handler_cls(self, message: Message) -> type[AbstractHandler]:
        header_content = self._get_header_content(message)
        handler_cls = self._handlers.get(header_content)

        if not handler_cls:
            raise HandlerError('Не найден соответствующий заголовку обработчик')

        return handler_cls

    def _get_header_content(self, message: Message) -> str or None:
        headers = message.headers()

        if not headers:
            raise HeadersError('В сообщении не переданы заголовки')

        for header in headers:
            key = header[0].lower()
            if key == self._HEADER_KEY.lower():
                return header[1].decode('utf-8')

        raise HeadersError(f'Заголовок {self._HEADER_KEY} не найден')
