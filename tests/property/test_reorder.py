"""AC-011-30: the hold-back function over any delivery order (``ems.actors.reorder``).

- Any permutation of the numbers ``expected … expected + n - 1``, with repeats at any place, comes
  out in order, each number once, carrying the message of its first delivery; every prefix of the
  deliveries releases the longest run of consecutive numbers it holds.
- ``reordered`` counts the deliveries that arrived early, ``stale`` the repeats; the final inbox is
  ``Inbox(expected + n)`` whatever the order (the counters are outside ``==``).
- With no start (``expected`` None), deliveries are held until ``begin(start)``, which drops the
  held numbers below ``start`` and releases the consecutive ones; numbers below ``start`` that
  arrive later are stale too.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ems.actors.reorder import Inbox, begin, in_order

# Numbers are step indices (quarter-hours since 1970, about 2e6 today) or sequence numbers (from 1).
first_numbers = st.integers(min_value=-(2**40), max_value=2**40)
message_counts = st.integers(min_value=0, max_value=30)

type Delivery = tuple[int, str]  # (number, message naming its delivery)


@st.composite
def deliveries(draw: st.DrawFn, first: int, n: int) -> list[Delivery]:
    """A permutation of ``first … first + n - 1`` with up to ``n`` repeats inserted anywhere."""
    order = draw(st.permutations(range(first, first + n)))
    repeats = draw(st.lists(st.sampled_from(order), max_size=n)) if n else []
    numbers = list(order)
    for number in repeats:
        numbers.insert(draw(st.integers(min_value=0, max_value=len(numbers))), number)
    return [(number, f"m{number}@{i}") for i, number in enumerate(numbers)]


@st.composite
def runs(draw: st.DrawFn) -> tuple[int, int, list[Delivery]]:
    first, n = draw(first_numbers), draw(message_counts)
    return first, n, draw(deliveries(first, n))


def _first_copies(sent: list[Delivery]) -> dict[int, str]:
    first: dict[int, str] = {}
    for number, message in sent:
        first.setdefault(number, message)
    return first


@given(case=runs())
def test_ac_011_30_any_order_releases_every_number_once_in_order(
    case: tuple[int, int, list[Delivery]],
) -> None:
    first, n, sent = case
    inbox: Inbox[str] = Inbox(expected=first)
    released: list[str] = []
    seen: set[int] = set()
    early = 0
    for i, (number, message) in enumerate(sent):
        expected_before = inbox.expected
        assert expected_before is not None
        if number > expected_before and number not in seen:
            early += 1
        seen.add(number)
        inbox, ready = in_order(inbox, number, message)
        released += ready
        # The prefix releases the longest run of consecutive numbers delivered so far.
        upto = first
        while upto in seen:
            upto += 1
        copies = _first_copies(sent[: i + 1])
        assert released == [copies[k] for k in range(first, upto)]
        assert inbox.expected == upto

    copies = _first_copies(sent)
    assert released == [copies[k] for k in range(first, first + n)]
    assert inbox == Inbox(expected=first + n)
    assert inbox.held == ()
    assert inbox.reordered == early
    assert inbox.stale == len(sent) - n


@given(first=first_numbers, n=message_counts, data=st.data())
def test_ac_011_30_the_final_inbox_does_not_depend_on_the_delivery_order(
    first: int, n: int, data: st.DataObject
) -> None:
    one = data.draw(deliveries(first, n), label="one")
    other = data.draw(deliveries(first, n), label="other")

    def fold(sent: list[Delivery]) -> tuple[Inbox[int], list[int]]:
        inbox: Inbox[int] = Inbox(expected=first)
        out: list[int] = []
        for number, _ in sent:
            inbox, ready = in_order(inbox, number, number)
            out += ready
        return inbox, out

    assert fold(one) == fold(other)


@given(data=st.data(), first=first_numbers, n=message_counts)
def test_ac_011_30_begin_releases_what_is_held_from_its_start(
    data: st.DataObject, first: int, n: int
) -> None:
    start = data.draw(st.integers(min_value=first, max_value=first + n), label="start")
    sent = data.draw(deliveries(first, n), label="sent")
    split = data.draw(st.integers(min_value=0, max_value=len(sent)), label="split")

    inbox: Inbox[str] = Inbox()
    for number, message in sent[:split]:
        inbox, ready = in_order(inbox, number, message)
        assert ready == ()  # nothing before the start is known
    assert inbox.expected is None
    distinct_before = {number for number, _ in sent[:split]}
    assert [number for number, _ in inbox.held] == sorted(distinct_before)
    assert inbox.stale == split - len(distinct_before)

    inbox, released_list = begin(inbox, start)
    released = list(released_list)
    for number, message in sent[split:]:
        inbox, ready = in_order(inbox, number, message)
        released += ready

    copies = _first_copies(sent)
    assert released == [copies[k] for k in range(start, first + n)]
    assert inbox == Inbox(expected=first + n)
    # Every delivery is applied once or dropped as stale; only early ones are held.
    assert inbox.stale == len(sent) - (first + n - start)
