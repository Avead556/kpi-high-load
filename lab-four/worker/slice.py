"""Synthetic slice and the windowing step: plain Python loops, no NumPy."""
import math

LEVELS = 4096
MAX_VALUE = LEVELS - 1
LCG_A, LCG_C, LCG_MASK = 1103515245, 12345, 0x7FFFFFFF


def generate(seed: int, size: int) -> list[int]:
    """x(0) = seed, x(i+1) = (a x(i) + c) mod 2^31, p(i) = x(i+1) >> 19."""
    pixels = [0] * (size * size)
    x = seed
    for i in range(size * size):
        x = (LCG_A * x + LCG_C) & LCG_MASK
        pixels[i] = x >> 19
    return pixels


def histogram(pixels: list[int]) -> list[int]:
    hist = [0] * LEVELS
    for p in pixels:
        hist[p] += 1
    return hist


def level_at(hist: list[int], q: float) -> int:
    """Smallest level whose cumulative count reaches q * n."""
    rank = q * sum(hist)
    cumulative = 0
    for level, count in enumerate(hist):
        cumulative += count
        if cumulative >= rank:
            return level
    return MAX_VALUE


def window(pixels: list[int], p_lo: int, p_hi: int) -> dict:
    """p' = 0 outside [p_lo, p_hi], else round(4095 (p - p_lo) / (p_hi - p_lo)), half up."""
    span = p_hi - p_lo
    out = [0] * len(pixels)
    removed = kept = total = squares = 0
    for i, p in enumerate(pixels):
        if p < p_lo or p > p_hi:
            removed += 1
            continue
        v = (2 * MAX_VALUE * (p - p_lo) + span) // (2 * span) if span else MAX_VALUE
        out[i] = v
        kept += 1
        total += v
        squares += v * v
    mean = total / kept if kept else 0.0
    std = math.sqrt(max(squares / kept - mean * mean, 0.0)) if kept else 0.0
    return {"removed": removed, "kept": kept, "mean": round(mean, 4), "std": round(std, 4)}


def process(seed: int, size: int, q_lo: float, q_hi: float, between=lambda: None) -> dict:
    """Three passes over the slice; `between` runs at each pass boundary and may raise to stop the task."""
    pixels = generate(seed, size)
    between()
    hist = histogram(pixels)
    p_lo, p_hi = level_at(hist, q_lo), level_at(hist, q_hi)
    between()
    stats = window(pixels, p_lo, p_hi)
    return {"p_lo": p_lo, "p_hi": p_hi, **stats, "q_lo": q_lo, "q_hi": q_hi, "size": size}
