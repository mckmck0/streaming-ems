"""Polish voivodeships for site sampling.

Bounding boxes are approximate (rounded to 0.1 degree) and only steer sampling; VERIFY against the GUGiK
PRG boundary register (free for any use) before using them for anything spatially precise. Weights are
rough shares for sampling households (population), PV and wind siting (south PV-heavy, coast wind-heavy).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Voivodeship:
    teryt: str
    name: str
    lat: tuple[float, float]
    lon: tuple[float, float]
    elevation_m: float
    dso: str
    cities: tuple[str, ...]
    postcode_prefixes: tuple[int, int]
    household_weight: float
    pv_weight: float
    wind_weight: float
    wind_scale_ms: float  # Weibull scale of the 10 m wind


VOIVODESHIPS: tuple[Voivodeship, ...] = (
    Voivodeship(
        "02",
        "dolnośląskie",
        (50.2, 51.7),
        (15.0, 17.7),
        200,
        "Tauron",
        ("Wrocław", "Wałbrzych", "Legnica"),
        (50, 59),
        7.6,
        1.1,
        0.6,
        3.1,
    ),
    Voivodeship(
        "04",
        "kujawsko-pomorskie",
        (52.4, 53.7),
        (17.3, 19.7),
        90,
        "Energa",
        ("Bydgoszcz", "Toruń", "Włocławek"),
        (85, 89),
        5.4,
        1.0,
        1.2,
        3.6,
    ),
    Voivodeship(
        "06",
        "lubelskie",
        (50.3, 52.2),
        (21.7, 24.1),
        200,
        "PGE",
        ("Lublin", "Zamość", "Chełm"),
        (20, 24),
        5.4,
        1.2,
        0.7,
        3.2,
    ),
    Voivodeship(
        "08",
        "lubuskie",
        (51.5, 53.0),
        (14.6, 16.3),
        80,
        "Enea",
        ("Zielona Góra", "Gorzów Wielkopolski"),
        (65, 68),
        2.6,
        1.0,
        0.9,
        3.4,
    ),
    Voivodeship(
        "10",
        "łódzkie",
        (50.9, 52.3),
        (18.1, 20.6),
        180,
        "PGE",
        ("Łódź", "Piotrków Trybunalski"),
        (90, 99),
        6.4,
        1.1,
        1.0,
        3.3,
    ),
    Voivodeship(
        "12",
        "małopolskie",
        (49.3, 50.4),
        (19.2, 21.3),
        300,
        "Tauron",
        ("Kraków", "Tarnów", "Nowy Sącz"),
        (30, 34),
        8.9,
        1.3,
        0.3,
        2.7,
    ),
    Voivodeship(
        "14",
        "mazowieckie",
        (51.1, 53.4),
        (19.4, 23.0),
        110,
        "PGE",
        ("Warszawa", "Radom", "Płock"),
        (0, 9),
        14.3,
        1.1,
        0.8,
        3.3,
    ),
    Voivodeship(
        "16",
        "opolskie",
        (50.0, 51.1),
        (17.0, 18.6),
        200,
        "Tauron",
        ("Opole", "Kędzierzyn-Koźle"),
        (45, 49),
        2.5,
        1.2,
        0.7,
        3.1,
    ),
    Voivodeship(
        "18",
        "podkarpackie",
        (49.1, 50.7),
        (21.2, 23.5),
        250,
        "PGE",
        ("Rzeszów", "Przemyśl", "Krosno"),
        (35, 39),
        5.5,
        1.3,
        0.4,
        2.8,
    ),
    Voivodeship(
        "20",
        "podlaskie",
        (52.4, 54.3),
        (21.7, 23.9),
        140,
        "PGE",
        ("Białystok", "Suwałki", "Łomża"),
        (15, 19),
        3.1,
        1.0,
        1.0,
        3.4,
    ),
    Voivodeship(
        "22",
        "pomorskie",
        (53.6, 54.7),
        (16.8, 19.5),
        80,
        "Energa",
        ("Gdańsk", "Gdynia", "Słupsk"),
        (80, 84),
        6.1,
        0.9,
        1.8,
        4.2,
    ),
    Voivodeship(
        "24",
        "śląskie",
        (49.5, 51.0),
        (18.1, 19.8),
        280,
        "Tauron",
        ("Katowice", "Częstochowa", "Gliwice"),
        (40, 44),
        11.6,
        1.2,
        0.4,
        2.9,
    ),
    Voivodeship(
        "26",
        "świętokrzyskie",
        (50.3, 51.2),
        (19.8, 21.7),
        250,
        "PGE",
        ("Kielce", "Ostrowiec Świętokrzyski"),
        (25, 29),
        3.1,
        1.2,
        0.5,
        3.0,
    ),
    Voivodeship(
        "28",
        "warmińsko-mazurskie",
        (53.2, 54.4),
        (19.3, 22.7),
        130,
        "Energa",
        ("Olsztyn", "Elbląg", "Ełk"),
        (10, 14),
        3.6,
        0.9,
        1.3,
        3.7,
    ),
    Voivodeship(
        "30",
        "wielkopolskie",
        (51.2, 53.6),
        (15.9, 18.9),
        90,
        "Enea",
        ("Poznań", "Kalisz", "Konin"),
        (60, 64),
        9.1,
        1.1,
        1.3,
        3.5,
    ),
    Voivodeship(
        "32",
        "zachodniopomorskie",
        (52.7, 54.5),
        (14.2, 16.8),
        50,
        "Enea",
        ("Szczecin", "Koszalin"),
        (70, 78),
        4.4,
        0.9,
        1.9,
        4.1,
    ),
)

BY_TERYT = {v.teryt: v for v in VOIVODESHIPS}

# 4-digit GS1 operator numbers inside PPE codes. Tauron's is confirmed (komunikat 11/2020); the others
# are synthetic placeholders (9xxx) so generated codes never collide with real metering points.
DSO_OPERATOR_PREFIX = {
    "Tauron": "3224",
    "PGE": "9001",
    "Enea": "9002",
    "Energa": "9003",
    "Stoen": "9004",
}
