"""ssl_compat.py — shared SSL context with Python 3.13+ strictness relaxed.

Python 3.13+ enables VERIFY_X509_STRICT by default in
ssl.create_default_context(). It rejects CA certs whose Basic Constraints
extension isn't marked critical — a pattern still common in older
intermediate certs (ArcGIS Online's DigiCert chain, Overpass API, some
USGS endpoints).

The result is "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed:
Basic Constraints of CA cert not marked critical (_ssl.c:1081)" from
urllib.request.urlopen when SSL context isn't passed explicitly.

This module provides `get_ssl_context()` which returns a context that:
  - Still validates the full chain
  - Still validates hostname
  - Still validates expiry
  - Just doesn't enforce strict RFC 5280 compliance on intermediates

Diagnosed 2026-05-22 LATE during DE virgin-state pipeline test
(load_pad_us_state.py + Overpass API both failing on Python 3.14).

Usage:
    from scripts.common.ssl_compat import get_ssl_context
    ctx = get_ssl_context()
    with urllib.request.urlopen(req, context=ctx, timeout=120) as r:
        ...
"""
from __future__ import annotations
import ssl


def get_ssl_context() -> ssl.SSLContext:
    """Return an SSL context with VERIFY_X509_STRICT relaxed.

    Safe to use for HTTPS calls to USGS / ArcGIS / Overpass / similar
    public endpoints whose cert chains pre-date RFC 5280 strictness.
    """
    ctx = ssl.create_default_context()
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx
