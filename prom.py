import logging 
import time
import os
from typing import Callable
from collections import namedtuple

from fastapi import FastAPI, Response
from prometheus_client import Counter, Histogram
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, \
    CollectorRegistry, generate_latest, multiprocess
from starlette.requests import Request
from starlette.middleware.base import BaseHTTPMiddleware


LOGGER = logging.getLogger('zenith_apigen.extras.prometheus')


ResponseInfo = namedtuple('ResponseInfo', ['exec_time', 'headers', 'status_code', 'status_group', 'endpoint', 'method'])


class PromMetricsMiddleware(BaseHTTPMiddleware):
    """Middleware used to handle registration and
    reporting of Prometheus metrics. Prometheus 
    metrics are implemented via a serious of functions,
    each of which takes and instance of the ResponseInfo
    tuple containing information about the processed
    request"""
    
    def __init__(self, app, metrics: Callable):
        super().__init__(app)
        self.metrics = metrics
    
    async def dispatch(self, request: Request, call_next):
        """Dispatch method used to handle execution
        of middleware. All requests are timed and
        and instance of ResponseInfo is generated
        containing data about the response. All
        metric handlers are then called with the 
        ResponseInfo instance.

        Parameters
        ----------
        request : Request
            FastAPI request instance
        call_next : _type_
            Callable used to process request
        """
        
        start = time.time()
        response = await call_next(request)
        # calculate execution time
        exec_time = time.time() - start
        # get request headers
        headers = request.headers
        # get status_code (including grouping)
        status_code = response.status_code
        status_group = str(status_code)[0] + 'xx'
        # get request method
        method = request.method.upper()
        endpoint = request.url.path
        
        info = ResponseInfo(exec_time=exec_time, 
                           headers=headers, 
                           status_code=status_code, 
                           status_group=status_group,
                           endpoint=endpoint,
                           method=method)
        # call metrics handler with required info instance
        for handler in self.metrics:
            handler(info)
        return response
        
        
def _get_registry():
    """Function used to generate Prometheus
    registry. If the prometheus_multiproc_dir
    setting is configured in the environment,
    a MultiProcessCollector instance is configured.
    Else the default registry is returned"""
    
    # retrieve value set in environment vars
    pmd = os.environ.get('prometheus_multiproc_dir')
    if pmd is not None:
        # check that directory is valid directory
        # and generate new registry for multiprocessing
        if os.path.isdir(pmd):
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
        else:
            raise ValueError(f'Unable to generate Prometheus registry: '
                             'invalid directory {pmd}')
        return registry
    return REGISTRY
            

def http_requests_total(service_name: str) -> Callable:
    """Metrics function used to implement
    a count for all incoming HTTP requests
    that are made to the server instance.

    Parameters
    ----------
    service_name : str
        name of service (used for service_name label)

    Returns
    -------
    Callable
        Closure used to implement metric
    """
    
    METRIC = Counter('http_requests_total', 
                     'Counter used to store total HTTP requests.',
                     ['service_name', 'status_code', 'status_group', 'endpoint', 'method', 'uid'])

    def instrument(info: ResponseInfo):
        """Closure used to handle instrumentation
        at metric level"""
        
        uid = info.headers.get('uid')
        # generate labels and increment counter
        labels = {'service_name': service_name,
                  'status_code': info.status_code,
                  'status_group': info.status_group,
                  'endpoint': info.endpoint,
                  'method': info.method,
                  'uid': uid}
        METRIC.labels(**labels).inc()
    return instrument


def latency(service_name: str) -> Callable:
    """Metrics function used to implement
    a histogram for all incoming HTTP requests
    that are made to the server instance.

    Parameters
    ----------
    service_name : str
        name of service (used for service_name label)

    Returns
    -------
    Callable
        Closure used to implement metric
    """
    
    
    METRIC = Histogram('http_request_duration_seconds', 
                       'Histogram measuring duration of HTTP requests in seconds',
                       ['service_name', 'status_code', 'status_group', 'endpoint', 'method', 'uid'])

    def instrument(info: ResponseInfo):
        """Closure used to handle instrumentation
        at metric level"""
        
        uid = info.headers.get('uid')
        # generate labels and increment counter
        labels = {'service_name': service_name,
                  'status_code': info.status_code,
                  'status_group': info.status_group,
                  'endpoint': info.endpoint,
                  'method': info.method,
                  'uid': uid}
        METRIC.labels(**labels).observe(info.exec_time)
    return instrument
            
            
METRIC_HANDLERS = [
    http_requests_total,
    latency
]


class PrometheusInstrumentator:
    """Instrumentator class used to configure
    Prometheus metrics for a given instance
    of a FastAPI application via the instrument_app
    method"""
    
    @staticmethod
    def instrument_app(app: FastAPI, service_name: str) -> FastAPI:
        """Method used to add standard set
        of prometheus metrics to API

        Parameters
        ----------
        app : FastAPI
            FastAPI app to add metrics to
        service_name : str
            name of service (used for service_name label)

        Returns
        -------
        FastAPI
            FastAPI app with added metrics
        """
        
        # generate metric handlers (fix service name)
        metrics = [m(service_name) for m in METRIC_HANDLERS]
        app.add_middleware(PromMetricsMiddleware, metrics=metrics)
        # get prometheus registry (requires use of 
        # local directory if multiprocessing is enabled)
        registry = _get_registry()
        
        @app.get('/metrics')
        def metrics() -> Response:
            """Closure used to handle metrics
            aggregation from prometheus scrape 
            jobs.

            Returns:
                Response: prometheus metrics
            """
            
            resp = Response(content=generate_latest(registry))
            resp.headers['Content-Type'] = CONTENT_TYPE_LATEST
            return resp
        return app
