"""
Optional Dependencies Wrapper
============================

Handles optional imports gracefully to avoid dependency issues.
"""

import logging
import warnings

logger = logging.getLogger(__name__)

# BigQuery support
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    BIGQUERY_AVAILABLE = True
    logger.info("BigQuery support available")
except ImportError as e:
    logger.info("BigQuery not available - offline mode only")
    BIGQUERY_AVAILABLE = False
    bigquery = None
    GoogleCloudError = Exception

# Pandas support
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    logger.info("Pandas support available")
except ImportError:
    logger.info("Pandas not available - using basic data structures")
    PANDAS_AVAILABLE = False
    pd = None

# PyArrow support
try:
    import pyarrow
    PYARROW_AVAILABLE = True
    logger.info("PyArrow support available")
except ImportError:
    logger.info("PyArrow not available - limited BigQuery functionality")
    PYARROW_AVAILABLE = False
    pyarrow = None

# LXML support for better HTML parsing
try:
    import lxml
    LXML_AVAILABLE = True
    logger.info("LXML support available")
except ImportError:
    logger.info("LXML not available - using html.parser")
    LXML_AVAILABLE = False
    lxml = None

def check_bigquery_requirements() -> bool:
    """Check if BigQuery requirements are met"""
    return BIGQUERY_AVAILABLE and PANDAS_AVAILABLE and PYARROW_AVAILABLE

def get_html_parser():
    """Get the best available HTML parser"""
    if LXML_AVAILABLE:
        return 'lxml'
    else:
        return 'html.parser'

def warn_missing_dependencies(feature: str, missing_deps: List[str]):
    """Warn about missing dependencies for a feature"""
    deps_str = ", ".join(missing_deps)
    warning_msg = f"{feature} requires: {deps_str}. Install with: pip install {deps_str}"
    warnings.warn(warning_msg, UserWarning)
    logger.warning(warning_msg)
