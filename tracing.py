import logging
import os
import functools
from typing import List

# disable tracing of metrics endpoints
os.environ['OTEL_PYTHON_FASTAPI_EXCLUDED_URLS'] = 'metrics'
        
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.trace import TracerProvider, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


LOGGER = logging.getLogger('zenith_apigen.extras.jaeger')


def get_headers(context: dict) -> dict:
    """Function used to turn opentelemetry
    context into request headers

    Parameters
    ----------
    context : dict
        request context and information

    Returns
    -------
    dict
        dict containing headers
    """

    headers = context.get('headers')
    return {k.decode(): v.decode() for k, v in headers}


def server_request_hook(headers: List[str], span: Span, context: dict):
    """Request hook used to add custom header
    spans for a given request. Headers are 
    retrieved at request time and the values
    (if present) are set as span attributes

    Parameters
    ----------
    headers : List[str]
        List of headers that should be set
    span : Span
        active jaeger span
    context : dict
        HTTP context
    """

    # get headers from request and parse
    request_headers = get_headers(context)
    if span and span.is_recording():
        for h in headers:
            # extract header and set in span
            value = request_headers.get(h)
            span.set_attribute(h, value if value else 'not set')


class JaegerInstrumentator:
    """Instrumentation class used to add
    Jaeger Tracing configuration to FastAPI
    instance"""

    @staticmethod
    def instrument_app(app: FastAPI,
                       service_name: str, 
                       jaeger_host: str = None, 
                       jaeger_port: int = 6831,
                       headers: List[str] = []) -> FastAPI:
        """Method used to add jaeger tracing to
        a FastAPI instance. A list of headers can be
        provided to be set as attributes in spans

        Parameters
        ----------
        app : FastAPI
            FastAPI instance
        service_name : str
            Name of service
        jaeger_host : str, optional
            Jaeger agent host, by default None
        jaeger_port : int, optional
            Jaeger agent port, by default 6831
        headers : List[str], optional
            List of headers to extract
                from request and set in spans, by default []

        Returns
        -------
        FastAPI
            FastAPI instance with tracing configured
        """

        # get jaeger host from environment if not set
        if jaeger_host is None:
            jaeger_host = os.environ.get('JAEGER_HOST', 'localhost')
        
        # generate new tracer provider and register
        resource = Resource.create({SERVICE_NAME: service_name})
        trace.set_tracer_provider(TracerProvider(resource=resource))
        jaeger_exporter = JaegerExporter(agent_host_name=jaeger_host, 
                                         agent_port=jaeger_port)
        # add processor and exporter
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter, max_export_batch_size=10)
        )
        # add instrumentor for jaeger with request hook
        request_hook = functools.partial(server_request_hook, headers)
        FastAPIInstrumentor.instrument_app(app, server_request_hook=request_hook)
        return app
