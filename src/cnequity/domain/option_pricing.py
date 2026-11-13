"""Vectorised option pricing on futures: Black-76 and Barone-Adesi–Whaley.

Everything here takes numpy arrays (one element per option) and returns
arrays, because the lake prices every listed strike of every product every
session — tens of thousands of rows a day — and a per-row Python solver would
make a full rebuild take hours.

- **European** options (CFFEX index options; SHFE copper and gold before their
  2022 series) use Black-76.
- **American** options on futures (every other commodity option) use the
  Barone-Adesi–Whaley quadratic approximation with zero cost of carry. Early
  exercise of a futures option only matters through the interest earned on
  the intrinsic value, so BAW is close to a lattice at a fraction of the cost.

Conventions, stated once: ``t`` is in years (calendar days / 365), ``r`` a
continuously compounded rate as a fraction, ``sigma`` a fraction. Greeks are
per unit move: ``vega`` per 1.00 of volatility, ``theta`` per year of calendar
time (negative for a long option as time passes), ``rho`` per 1.00 of rate.

The normal CDF is Zelen & Severo's rational approximation (Abramowitz &
Stegun 26.2.17, absolute error below 7.5e-8), which keeps implied volatility
accurate to well under 1e-6 without adding a scientific library.
"""

from __future__ import annotations

import numpy as np

#: Bump when a price, implied volatility or Greek this module returns changes
#: for the same inputs. Stored Greeks record it, so a bump recomputes them all;
#: a refactor that returns the same numbers must leave it alone.
MODEL_VERSION = 1

__all__ = [
    "baw",
    "black76",
    "greeks",
    "implied_vol",
    "norm_cdf",
]

_SQRT_2PI = np.sqrt(2.0 * np.pi)


def norm_pdf(x: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * x * x) / _SQRT_2PI


def norm_cdf(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    k = 1.0 / (1.0 + 0.2316419 * np.abs(x))
    poly = k * (
        0.319381530 + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429)))
    )
    upper = 1.0 - norm_pdf(x) * poly
    return np.where(x >= 0, upper, 1.0 - upper)


def _d1(f, k, t, sigma):
    return (np.log(f / k) + 0.5 * sigma * sigma * t) / (sigma * np.sqrt(t))


def black76(f, k, t, r, sigma, is_call):
    """European option on a futures price."""
    f, k, t, r, sigma = (np.asarray(v, dtype=float) for v in (f, k, t, r, sigma))
    d1 = _d1(f, k, t, sigma)
    d2 = d1 - sigma * np.sqrt(t)
    discount = np.exp(-r * t)
    call = discount * (f * norm_cdf(d1) - k * norm_cdf(d2))
    put = discount * (k * norm_cdf(-d2) - f * norm_cdf(-d1))
    return np.where(is_call, call, put)


def _critical(k, t, r, sigma, is_call, q):
    """Early-exercise boundary by Newton, zero carry (Haug, 2nd ed., §3.10)."""
    b = 0.0
    n = 2.0 * b / (sigma * sigma)
    m = 2.0 * r / (sigma * sigma)
    root_u = np.sqrt((n - 1.0) ** 2 + 4.0 * m)
    q_inf = np.where(is_call, 0.5 * (-(n - 1.0) + root_u), 0.5 * (-(n - 1.0) - root_u))
    s_inf = k / (1.0 - 1.0 / q_inf)
    vol_t = sigma * np.sqrt(t)
    h_call = -(b * t + 2.0 * vol_t) * k / (s_inf - k)
    h_put = (b * t - 2.0 * vol_t) * k / (k - s_inf)
    s = np.where(
        is_call,
        k + (s_inf - k) * (1.0 - np.exp(h_call)),
        s_inf + (k - s_inf) * np.exp(h_put),
    )
    carry = np.exp((b - r) * t)
    for _ in range(100):
        d1 = (np.log(s / k) + (b + 0.5 * sigma * sigma) * t) / vol_t
        european = black76(s, k, t, r, sigma, is_call)
        call_rhs = european + (1.0 - carry * norm_cdf(d1)) * s / q
        call_b = carry * norm_cdf(d1) * (1.0 - 1.0 / q) + (1.0 - carry * norm_pdf(d1) / vol_t) / q
        put_rhs = european - (1.0 - carry * norm_cdf(-d1)) * s / q
        put_b = -carry * norm_cdf(-d1) * (1.0 - 1.0 / q) - (1.0 + carry * norm_pdf(d1) / vol_t) / q
        new = np.where(
            is_call,
            (k + call_rhs - call_b * s) / (1.0 - call_b),
            (k - put_rhs + put_b * s) / (1.0 + put_b),
        )
        new = np.clip(new, 1e-10, None)
        done = np.all(np.abs(new - s) <= 1e-9 * np.maximum(1.0, k))
        s = new
        if done:
            break
    return s


def baw(f, k, t, r, sigma, is_call):
    """American option on a futures price (Barone-Adesi–Whaley, b = 0)."""
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        return _baw(f, k, t, r, sigma, is_call)


def _baw(f, k, t, r, sigma, is_call):
    f, k, t, r, sigma = (np.asarray(v, dtype=float) for v in (f, k, t, r, sigma))
    is_call = np.asarray(is_call, dtype=bool)
    european = black76(f, k, t, r, sigma, is_call)
    rate = np.maximum(r, 1e-12)
    m = 2.0 * rate / (sigma * sigma)
    kk = 1.0 - np.exp(-rate * t)
    root = np.sqrt(1.0 + 4.0 * m / kk)
    q = np.where(is_call, 0.5 * (1.0 + root), 0.5 * (1.0 - root))
    boundary = _critical(k, t, rate, sigma, is_call, q)
    d1 = _d1(boundary, k, t, sigma)
    discount = np.exp(-rate * t)
    a = np.where(
        is_call,
        (boundary / q) * (1.0 - discount * norm_cdf(d1)),
        -(boundary / q) * (1.0 - discount * norm_cdf(-d1)),
    )
    early = np.where(
        is_call,
        np.where(f < boundary, european + a * (f / boundary) ** q, f - k),
        np.where(f > boundary, european + a * (f / boundary) ** q, k - f),
    )
    # With no positive rate there is nothing to gain from exercising early.
    return np.where(r > 0, np.maximum(early, european), european)


def price(model, f, k, t, r, sigma, is_call):
    return (
        baw(f, k, t, r, sigma, is_call) if model == "baw" else black76(f, k, t, r, sigma, is_call)
    )


def implied_vol(model, target, f, k, t, r, is_call, *, lo=1e-4, hi=5.0, iterations=80):
    """Volatility that reprices *target*, by vectorised bisection.

    Returns ``(sigma, status)``. ``status`` is ``ok``; ``below_intrinsic`` when
    no volatility can price that low (the settlement sits under what exercise
    alone is worth); or ``above_bound`` when even ``hi`` cannot reach it.
    """
    target = np.asarray(target, dtype=float)
    low = np.full(target.shape, lo)
    high = np.full(target.shape, hi)
    p_lo = price(model, f, k, t, r, low, is_call)
    p_hi = price(model, f, k, t, r, high, is_call)
    status = np.full(target.shape, "ok", dtype=object)
    status[target < p_lo - 1e-10] = "below_intrinsic"
    status[target > p_hi + 1e-10] = "above_bound"
    for _ in range(iterations):
        mid = 0.5 * (low + high)
        p_mid = price(model, f, k, t, r, mid, is_call)
        too_high = p_mid > target
        high = np.where(too_high, mid, high)
        low = np.where(too_high, low, mid)
    sigma = 0.5 * (low + high)
    sigma = np.where(status == "ok", sigma, np.nan)
    return sigma, status


def greeks(model, f, k, t, r, sigma, is_call):
    """Delta, gamma, vega, theta and rho; analytic for Black-76, bumped for BAW."""
    f, k, t, r, sigma = (np.asarray(v, dtype=float) for v in (f, k, t, r, sigma))
    is_call = np.asarray(is_call, dtype=bool)
    if model == "black76":
        d1 = _d1(f, k, t, sigma)
        d2 = d1 - sigma * np.sqrt(t)
        discount = np.exp(-r * t)
        value = black76(f, k, t, r, sigma, is_call)
        delta = np.where(is_call, discount * norm_cdf(d1), -discount * norm_cdf(-d1))
        gamma = discount * norm_pdf(d1) / (f * sigma * np.sqrt(t))
        vega = f * discount * norm_pdf(d1) * np.sqrt(t)
        theta = -f * discount * norm_pdf(d1) * sigma / (2.0 * np.sqrt(t)) + r * value
        rho = -t * value
        del d2
        return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta, "rho": rho}
    h = f * 1e-4
    up = price(model, f + h, k, t, r, sigma, is_call)
    down = price(model, f - h, k, t, r, sigma, is_call)
    mid = price(model, f, k, t, r, sigma, is_call)
    dt = np.minimum(1.0 / 365.0, t / 2.0)
    later = price(model, f, k, t - dt, r, sigma, is_call)
    return {
        "delta": (up - down) / (2.0 * h),
        "gamma": (up - 2.0 * mid + down) / (h * h),
        "vega": (
            price(model, f, k, t, r, sigma + 1e-4, is_call)
            - price(model, f, k, t, r, sigma - 1e-4, is_call)
        )
        / 2e-4,
        "theta": (later - mid) / dt,
        "rho": (
            price(model, f, k, t, r + 1e-4, sigma, is_call)
            - price(model, f, k, t, np.maximum(r - 1e-4, 0.0), sigma, is_call)
        )
        / (r + 1e-4 - np.maximum(r - 1e-4, 0.0)),
    }
