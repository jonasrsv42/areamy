"""Generate the README banner, `assets/stream.svg`.

A signal flows like a stream through two nodes: raw on the way in, clean on the way out.

Usage, from the repository root:

    python3 scripts/banner.py
"""

import math
from collections.abc import Callable
from pathlib import Path

OUTPUT = Path("assets") / "stream.svg"

WIDTH = 880
HEIGHT = 104
CENTER_Y = 52
# Every wave repeats every PERIOD px, so shifting one by PERIOD loops without a seam.
PERIOD = 96
# Where a wave eases into or out of a node.
FADE = 40

Wave = Callable[[float, float], float]


def raw(x: float, phase: float = 0.0) -> float:
    """Busy and peaky: the signal as it comes in."""
    t = 2 * math.pi * (x + phase) / PERIOD
    envelope = 0.55 + 0.45 * abs(math.sin(t / 2))
    return envelope * (
        22 * math.sin(t * 3) + 11 * math.sin(t * 7 + 1.3) + 6 * math.sin(t * 13 + 0.4)
    )


def cleaner(x: float, phase: float = 0.0) -> float:
    """After the first node."""
    t = 2 * math.pi * (x + phase) / PERIOD
    return 20 * math.sin(t * 2) + 6 * math.sin(t * 4 + 0.8)


def smooth(x: float, phase: float = 0.0) -> float:
    """After the second node: the signal as it goes out."""
    t = 2 * math.pi * (x + phase) / PERIOD
    return 18 * math.sin(t)


# (start, end, wave) of each stretch of the stream, and the nodes between them.
SEGMENTS: list[tuple[int, int, Wave]] = [(0, 250, raw), (290, 560, cleaner), (600, WIDTH, smooth)]
NODES = [270, 580]

# (phase, opacity, stroke width) of each layer of the ribbon.
LAYERS = [(0, 1.0, 2.4), (5, 0.5, 1.6), (-6, 0.35, 1.2), (11, 0.2, 1.0)]


def polyline(wave: Wave, phase: float, scale: float, start: int, end: int, step: int = 2) -> str:
    """An SVG path tracing `wave` at `phase`, scaled by `scale`, from `start` to `end`."""
    points = [
        f"{x:.1f},{CENTER_Y + wave(x, phase) * scale:.2f}" for x in range(start, end + 1, step)
    ]
    return "M" + " L".join(points)


def ribbon(wave: Wave, start: int, end: int, index: int) -> str:
    """The same wave at small phase offsets: reads as water, still reads as a signal.

    Drawn in white for the mask: the colour comes from one fixed gradient under it, so it
    doesn't drift as the waves move.
    """
    paths = []
    for layer, (phase, opacity, width) in enumerate(LAYERS):
        scale = 1 - 0.12 * layer
        shape = polyline(wave, phase, scale, start - PERIOD, end + PERIOD)
        # Each layer at its own speed, so the ribbon shimmers rather than sliding as one.
        seconds = 2.4 + layer * 0.35
        motion = (
            f'<animateTransform attributeName="transform" type="translate" '
            f'from="0 0" to="{PERIOD} 0" dur="{seconds:.2f}s" repeatCount="indefinite"/>'
        )
        paths.append(
            f'<path d="{shape}" fill="none" stroke="#fff" stroke-width="{width}" '
            f'stroke-linecap="round" stroke-linejoin="round" opacity="{opacity}">{motion}</path>'
        )
    return f'<g clip-path="url(#clip{index})" mask="url(#fade{index})">' + "".join(paths) + "</g>"


def node(center_x: int) -> str:
    """A rounded stone the stream passes through."""
    return (
        f'<rect x="{center_x - 20}" y="{CENTER_Y - 22}" width="40" height="44" rx="12" '
        f'fill="url(#stone)" stroke="#2f80ed" stroke-opacity="0.55" stroke-width="1.5"/>'
        f'<circle cx="{center_x}" cy="{CENTER_Y}" r="5" fill="#2f80ed" opacity="0.9"/>'
    )


def fade(index: int, start: int, end: int, fade_in: bool, fade_out: bool) -> str:
    """A mask easing a stretch in after a node and out before the next one."""
    width = end - start
    full_from = FADE / width if fade_in else 0
    full_to = 1 - FADE / width if fade_out else 1
    first = "#000" if fade_in else "#fff"
    last = "#000" if fade_out else "#fff"
    return (
        f'<linearGradient id="fadeg{index}" x1="{start}" x2="{end}" y1="0" y2="0" '
        f'gradientUnits="userSpaceOnUse">'
        f'<stop offset="0" stop-color="{first}"/>'
        f'<stop offset="{full_from:.3f}" stop-color="#fff"/>'
        f'<stop offset="{full_to:.3f}" stop-color="#fff"/>'
        f'<stop offset="1" stop-color="{last}"/></linearGradient>'
        f'<mask id="fade{index}" maskUnits="userSpaceOnUse" x="{start}" y="0" '
        f'width="{width}" height="{HEIGHT}">'
        f'<rect x="{start}" y="0" width="{width}" height="{HEIGHT}" fill="url(#fadeg{index})"/>'
        f"</mask>"
    )


def banner() -> str:
    """The whole SVG."""
    definitions = [
        f'<linearGradient id="flow" x1="0" x2="{WIDTH}" y1="0" y2="0" '
        f'gradientUnits="userSpaceOnUse">'
        '<stop offset="0" stop-color="#22c1c3"/><stop offset="0.5" stop-color="#2f80ed"/>'
        '<stop offset="1" stop-color="#7b61ff"/></linearGradient>',
        '<linearGradient id="stone" x1="0" x2="0" y1="0" y2="1">'
        '<stop offset="0" stop-color="#2f80ed" stop-opacity="0.22"/>'
        '<stop offset="1" stop-color="#7b61ff" stop-opacity="0.12"/></linearGradient>',
    ]
    ribbons = []
    last = len(SEGMENTS) - 1
    for index, (start, end, wave) in enumerate(SEGMENTS):
        definitions.append(
            f'<clipPath id="clip{index}"><rect x="{start}" y="0" '
            f'width="{end - start}" height="{HEIGHT}"/></clipPath>'
        )
        definitions.append(fade(index, start, end, index > 0, index < last))
        ribbons.append(ribbon(wave, start, end, index))
    definitions.append(
        f'<mask id="waves" maskUnits="userSpaceOnUse" x="0" y="0" '
        f'width="{WIDTH}" height="{HEIGHT}">{"".join(ribbons)}</mask>'
    )
    body = [f'<rect width="{WIDTH}" height="{HEIGHT}" fill="url(#flow)" mask="url(#waves)"/>']
    body.extend(node(center_x) for center_x in NODES)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="{WIDTH}" height="{HEIGHT}" role="img" '
        f'aria-label="Areamy: data flowing through a streaming graph">'
        f"<defs>{''.join(definitions)}</defs>{''.join(body)}</svg>"
    )


def main() -> None:
    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(banner())


if __name__ == "__main__":
    main()
