import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable

WidgetCallable = Callable[..., Any]
AsyncWidgetCallable = Callable[..., Awaitable[Any]]

WIDGETS: dict[str, dict[str, Any]] = {}


def register_widget(config: dict[str, Any]):
    """
    Decorator that registers widget metadata and returns a wrapped callable.

    Mirrors the pattern used in OpenBB examples while keeping dependencies local.
    """

    def decorator(func: WidgetCallable | AsyncWidgetCallable):
        endpoint = config.get("endpoint")
        if endpoint:
            widget_id = config.setdefault("widgetId", endpoint)
            WIDGETS[widget_id] = config

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return sync_wrapper

    return decorator
