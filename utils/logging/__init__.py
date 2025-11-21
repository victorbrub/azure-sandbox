"""Logging Utilities

Provides structured logging utilities for Azure data engineering projects
with support for different log levels, formatters, and output destinations.
"""

import logging
import structlog
from typing import Optional, Dict, Any
import sys
from pathlib import Path


def configure_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    structured: bool = True,
    json_logs: bool = False
) -> None:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to
        structured: Use structured logging with structlog
        json_logs: Output logs in JSON format
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Basic logging configuration
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        # Create log directory if needed
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    if structured:
        # Configure structlog
        if json_logs:
            renderer = structlog.processors.JSONRenderer()
        else:
            renderer = structlog.dev.ConsoleRenderer()
        
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                renderer,
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        
        # Configure standard logging to work with structlog
        logging.basicConfig(
            format="%(message)s",
            level=level,
            handlers=handlers,
            force=True
        )
    else:
        # Standard logging format
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(
            format=log_format,
            level=level,
            handlers=handlers,
            force=True
        )
    
    # Set Azure SDK logging to WARNING to reduce noise
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("msal").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str, use_structlog: bool = True) -> Any:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (usually __name__)
        use_structlog: Use structlog instead of standard logging
        
    Returns:
        Logger instance
    """
    if use_structlog:
        return structlog.get_logger(name)
    else:
        return logging.getLogger(name)


class LogContext:
    """Context manager for adding context to structured logs."""
    
    def __init__(self, logger: Any, **context: Any):
        """
        Initialize log context.
        
        Args:
            logger: Structlog logger instance
            **context: Key-value pairs to add to log context
        """
        self.logger = logger
        self.context = context
        self.bound_logger = None
    
    def __enter__(self):
        """Enter the context and bind context to logger."""
        self.bound_logger = self.logger.bind(**self.context)
        return self.bound_logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        pass


class OperationLogger:
    """Helper class for logging operations with timing and status."""
    
    def __init__(self, logger: Any, operation_name: str, **context: Any):
        """
        Initialize operation logger.
        
        Args:
            logger: Logger instance
            operation_name: Name of the operation
            **context: Additional context to log
        """
        self.logger = logger
        self.operation_name = operation_name
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        """Start the operation and log."""
        import time
        self.start_time = time.time()
        
        if hasattr(self.logger, 'bind'):
            # Structlog
            self.logger.info(
                f"Starting operation: {self.operation_name}",
                operation=self.operation_name,
                **self.context
            )
        else:
            # Standard logging
            self.logger.info(
                f"Starting operation: {self.operation_name}",
                extra={"operation": self.operation_name, **self.context}
            )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End the operation and log result."""
        import time
        duration = time.time() - self.start_time
        
        if exc_type is None:
            # Success
            if hasattr(self.logger, 'bind'):
                self.logger.info(
                    f"Completed operation: {self.operation_name}",
                    operation=self.operation_name,
                    duration_seconds=duration,
                    status="success",
                    **self.context
                )
            else:
                self.logger.info(
                    f"Completed operation: {self.operation_name} in {duration:.2f}s",
                    extra={
                        "operation": self.operation_name,
                        "duration_seconds": duration,
                        "status": "success",
                        **self.context
                    }
                )
        else:
            # Failure
            if hasattr(self.logger, 'bind'):
                self.logger.error(
                    f"Failed operation: {self.operation_name}",
                    operation=self.operation_name,
                    duration_seconds=duration,
                    status="failed",
                    error=str(exc_val),
                    **self.context
                )
            else:
                self.logger.error(
                    f"Failed operation: {self.operation_name} after {duration:.2f}s: {exc_val}",
                    extra={
                        "operation": self.operation_name,
                        "duration_seconds": duration,
                        "status": "failed",
                        "error": str(exc_val),
                        **self.context
                    }
                )
        
        return False  # Don't suppress exceptions


def log_function_call(logger: Any):
    """
    Decorator to log function calls with arguments and results.
    
    Args:
        logger: Logger instance
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            
            if hasattr(logger, 'bind'):
                logger.debug(
                    f"Calling function: {func_name}",
                    function=func_name,
                    args=str(args)[:100],  # Truncate long args
                    kwargs=str(kwargs)[:100]
                )
            else:
                logger.debug(
                    f"Calling function: {func_name}",
                    extra={
                        "function": func_name,
                        "args": str(args)[:100],
                        "kwargs": str(kwargs)[:100]
                    }
                )
            
            try:
                result = func(*args, **kwargs)
                
                if hasattr(logger, 'bind'):
                    logger.debug(
                        f"Function completed: {func_name}",
                        function=func_name,
                        result_type=type(result).__name__
                    )
                else:
                    logger.debug(
                        f"Function completed: {func_name}",
                        extra={
                            "function": func_name,
                            "result_type": type(result).__name__
                        }
                    )
                
                return result
            except Exception as e:
                if hasattr(logger, 'bind'):
                    logger.error(
                        f"Function failed: {func_name}",
                        function=func_name,
                        error=str(e)
                    )
                else:
                    logger.error(
                        f"Function failed: {func_name}: {e}",
                        extra={
                            "function": func_name,
                            "error": str(e)
                        }
                    )
                raise
        
        return wrapper
    return decorator
