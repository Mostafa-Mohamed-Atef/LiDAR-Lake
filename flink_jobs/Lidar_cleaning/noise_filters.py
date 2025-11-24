import math
from typing import Dict, Tuple


COORD_LIMITS = {
    "x": (-2000.0, 2000.0),
    "y": (-2000.0, 2000.0),
    "z": (-200.0, 500.0),
}

INTENSITY_RANGE = (0.0, 65535.0)
VALID_CLASSES = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}


def _clamp(value: float, bounds: Tuple[float, float]) -> bool:
    low, high = bounds
    return low <= value <= high


def is_valid_point(point: Dict) -> Tuple[bool, str]:
    """
    Validate a LiDAR point dictionary.

    Returns (True, "") when the point is valid, otherwise (False, reason).
    """
    try:
        x = float(point["x"])
        y = float(point["y"])
        z = float(point["z"])
        intensity = float(point.get("intensity", 0))
        classification = int(point.get("classification", -1))
    except (ValueError, TypeError, KeyError) as exc:
        return False, f"parse_error:{exc}"

    if not _clamp(x, COORD_LIMITS["x"]) or not _clamp(y, COORD_LIMITS["y"]) or not _clamp(z, COORD_LIMITS["z"]):
        return False, "coord_out_of_bounds"

    if not _clamp(intensity, INTENSITY_RANGE):
        return False, "intensity_out_of_range"

    if classification not in VALID_CLASSES:
        return False, "invalid_class"

    return True, ""

