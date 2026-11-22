"""AC-011-30: the one hold-back function every receiver uses (``ems.actors.reorder``; plan §5).

A receiver applies messages in number order (``t`` or ``seq``): an early message is held and counted
in ``reordered``; a stale one (already applied, or a number already held) is dropped and counted in
``stale``; with no start yet (``expected`` None) everything is held until ``begin`` sets it. The two
counters depend on arrival order only, so they stay outside ``==``.
"""

from __future__ import annotations

from ems.actors.reorder import Inbox, begin, in_order


def test_ac_011_30_the_expected_number_is_released_at_once() -> None:
    inbox, ready = in_order(Inbox[str](expected=5), 5, "m5")
    assert ready == ("m5",)
    assert (inbox.expected, inbox.held, inbox.reordered, inbox.stale) == (6, (), 0, 0)


def test_ac_011_30_an_early_message_is_held_counted_and_released_in_order() -> None:
    inbox, ready = in_order(Inbox[str](expected=5), 7, "m7")
    assert ready == ()
    assert (inbox.expected, inbox.held, inbox.reordered) == (5, ((7, "m7"),), 1)

    inbox, ready = in_order(inbox, 6, "m6")
    assert ready == ()
    assert inbox.held == ((6, "m6"), (7, "m7"))  # sorted by number
    assert inbox.reordered == 2

    inbox, ready = in_order(inbox, 5, "m5")
    assert ready == ("m5", "m6", "m7")
    assert (inbox.expected, inbox.held, inbox.reordered, inbox.stale) == (8, (), 2, 0)


def test_ac_011_30_an_applied_number_is_dropped_as_stale() -> None:
    inbox, _ = in_order(Inbox[str](expected=5), 5, "m5")
    after, ready = in_order(inbox, 5, "m5 again")
    assert ready == ()
    assert after.stale == 1 and after.reordered == 0
    assert (after.expected, after.held) == (inbox.expected, inbox.held)

    after, ready = in_order(after, 2, "m2")  # far behind
    assert ready == () and after.stale == 2


def test_ac_011_30_a_number_already_held_is_dropped_and_the_first_copy_kept() -> None:
    inbox, _ = in_order(Inbox[str](expected=5), 7, "first")
    inbox, ready = in_order(inbox, 7, "second")
    assert ready == ()
    assert inbox.held == ((7, "first"),)
    assert (inbox.reordered, inbox.stale) == (1, 1)

    _, ready = in_order(in_order(inbox, 5, "m5")[0], 6, "m6")
    assert ready == ("m6", "first")


def test_ac_011_30_without_a_start_everything_is_held() -> None:
    inbox: Inbox[str] = Inbox()
    assert inbox.expected is None
    for number in (3, 1, 2):
        inbox, ready = in_order(inbox, number, f"m{number}")
        assert ready == ()
    assert inbox.expected is None
    assert inbox.held == ((1, "m1"), (2, "m2"), (3, "m3"))
    assert inbox.reordered == 3  # each arrived before the start was known


def test_ac_011_30_begin_drops_older_held_numbers_and_releases_the_consecutive_ones() -> None:
    inbox: Inbox[str] = Inbox()
    for number in (7, 2, 4, 5):
        inbox, _ = in_order(inbox, number, f"m{number}")

    inbox, ready = begin(inbox, 4)
    assert ready == ("m4", "m5")
    assert inbox.expected == 6
    assert inbox.held == ((7, "m7"),)
    assert inbox.stale == 1  # m2 lies below the start

    inbox, ready = in_order(inbox, 6, "m6")
    assert ready == ("m6", "m7") and inbox.expected == 8


def test_ac_011_30_begin_on_an_empty_inbox_only_sets_the_start() -> None:
    inbox, ready = begin(Inbox[str](), 10)
    assert ready == ()
    assert inbox == Inbox(expected=10)
    assert (inbox.reordered, inbox.stale) == (0, 0)


def test_ac_011_30_begin_drops_every_held_number_below_the_start() -> None:
    inbox: Inbox[str] = Inbox()
    for number in (1, 2, 3):
        inbox, _ = in_order(inbox, number, f"m{number}")
    inbox, ready = begin(inbox, 4)
    assert ready == ()
    assert inbox == Inbox(expected=4)
    assert inbox.stale == 3


def test_ac_011_30_counters_are_outside_equality() -> None:
    held = ((9, "m9"),)
    a = Inbox(expected=8, held=held, reordered=0, stale=0)
    b = Inbox(expected=8, held=held, reordered=5, stale=3)
    assert a == b
    assert hash(a) == hash(b)
    assert a != Inbox(expected=8, held=(), reordered=0, stale=0)  # held is compared
    assert a != Inbox(expected=9, held=held)  # expected is compared


def test_ac_011_30_two_delivery_orders_end_in_equal_inboxes() -> None:
    forward: Inbox[str] = Inbox(expected=0)
    for number in range(4):
        forward, _ = in_order(forward, number, f"m{number}")
    backward: Inbox[str] = Inbox(expected=0)
    for number in (3, 2, 1, 0, 2):
        backward, _ = in_order(backward, number, f"m{number}")
    assert (forward.reordered, forward.stale) == (0, 0)
    assert (backward.reordered, backward.stale) == (3, 1)
    assert forward == backward == Inbox(expected=4)
