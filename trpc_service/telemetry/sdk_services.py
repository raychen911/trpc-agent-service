"""Transparent tracing proxies for SDK Session/Memory services.

Only the object the Runner receives is wrapped; the proxy delegates every
attribute to the real service.  Coroutine methods the Runner actually calls
get one fixed span (``state.session`` / ``state.memory``) with a single
low-cardinality ``operation`` attribute naming the method — no session id,
user id, app name or payload is ever attached.  Non-callable attributes
(e.g. ``memory_service.enabled``) and private members pass through unchanged,
so the SDK sees the same interface it would on the real object.

The SDK validates the services it receives with pydantic ``isinstance``
against ``SessionServiceABC`` / ``MemoryServiceABC`` (InvocationContext), so
the proxy classes are built to inherit the corresponding ABC with concrete
non-abstract stubs; every member the inherited ABC would otherwise resolve
is pre-bound to the real target in the instance dict, and attribute lookup
is dict-first so ABC-level properties cannot shadow target values.
"""

from __future__ import annotations

import functools
import inspect

from trpc_agent_sdk.abc import MemoryServiceABC, SessionServiceABC

from .runtime import ATTR_OPERATION, SPAN_STATE_MEMORY, SPAN_STATE_SESSION, safe_span


class _TracedServiceProxy:
    """Attribute-delegating proxy that traces called coroutine methods."""

    def __init__(self, target, tracer, span_name: str) -> None:
        d = self.__dict__
        d["_proxy_target"] = target
        d["_proxy_tracer"] = tracer
        d["_proxy_span_name"] = span_name
        # Neutralise members the proxy's own class (i.e. the mixed-in ABC)
        # would resolve before __getattr__ ever runs: pre-bind every such
        # name the target actually provides — coroutine methods get the
        # tracing wrapper, everything else delegates unchanged.
        for klass in type(self).__mro__:
            if klass in (object, _TracedServiceProxy):
                continue
            for name in vars(klass):
                if name.startswith("__") or name in d:
                    continue
                try:
                    attr = getattr(target, name)
                except AttributeError:
                    continue
                d[name] = self._resolve(name, attr)

    @staticmethod
    def _make_wrapper(name, attr, tracer, span_name):

        @functools.wraps(attr)
        async def _traced(*args, **kwargs):
            with safe_span(tracer, span_name, attributes={ATTR_OPERATION: name}):
                return await attr(*args, **kwargs)

        return _traced

    def _resolve(self, name: str, attr):
        if name.startswith("_") or not callable(attr) or not inspect.iscoroutinefunction(attr):
            return attr
        return self._make_wrapper(
            name,
            attr,
            self.__dict__["_proxy_tracer"],
            self.__dict__["_proxy_span_name"],
        )

    def __getattribute__(self, name: str):
        # Dict-first so pre-bound target members win over ABC methods and
        # properties (data descriptors would otherwise shadow the dict).
        if name.startswith("__") or name.startswith("_proxy_"):
            return object.__getattribute__(self, name)
        d = object.__getattribute__(self, "__dict__")
        if name in d:
            return d[name]
        return object.__getattribute__(self, name)

    def __getattr__(self, name: str):
        # Names absent from both the instance dict and the class resolve
        # against the real service (and cache traced wrappers lazily).
        target = self.__dict__["_proxy_target"]
        attr = getattr(target, name)
        return self.__dict__.setdefault(name, self._resolve(name, attr))

    def __repr__(self) -> str:
        return f"<traced {type(self.__dict__['_proxy_target']).__name__}>"


def _abstract_stub(name: str):
    """Namespace entry that neutralises one abstract method.

    Non-abstract so the ABC machinery considers the class concrete, yet any
    call surfaces the target's missing attribute the same way plain
    delegation would."""

    def _missing(self, *args, **kwargs):
        raise AttributeError(name)

    _missing.__name__ = name
    return _missing


_PROXY_CLASSES: dict[type, type] = {}


def _proxy_class(abc: type) -> type:
    cls = _PROXY_CLASSES.get(abc)
    if cls is None:
        namespace = {name: _abstract_stub(name) for name in getattr(abc, "__abstractmethods__", frozenset())}
        cls = type(f"Traced{abc.__name__}Proxy", (_TracedServiceProxy, abc), namespace)
        _PROXY_CLASSES[abc] = cls
    return cls


def instrument_session_service(session_service, tracer):
    """Wrap a SessionService so Runner calls produce ``state.session`` spans.

    The returned proxy passes ``isinstance(x, SessionServiceABC)`` because
    the SDK's InvocationContext validates it.
    """
    if tracer is None or session_service is None:
        return session_service
    return _proxy_class(SessionServiceABC)(session_service, tracer, SPAN_STATE_SESSION)


def instrument_memory_service(memory_service, tracer):
    """Wrap a MemoryService so calls produce ``state.memory`` spans."""
    if tracer is None or memory_service is None:
        return memory_service
    return _proxy_class(MemoryServiceABC)(memory_service, tracer, SPAN_STATE_MEMORY)


__all__ = ["instrument_memory_service", "instrument_session_service"]
