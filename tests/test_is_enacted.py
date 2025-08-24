import sys
import pathlib

# Add backend module path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1] / "backend"))

from main import is_enacted


def test_is_enacted_true_for_public_law_range():
    assert is_enacted(36000)
    assert is_enacted(36500)
    assert is_enacted(39999)


def test_is_enacted_true_for_private_law_range():
    assert is_enacted(41000)
    assert is_enacted(44999)


def test_is_enacted_false_outside_ranges():
    for code in [35999, 40001, 45000, None, "invalid"]:
        assert not is_enacted(code)
