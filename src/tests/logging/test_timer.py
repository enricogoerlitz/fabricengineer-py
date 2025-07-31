import time
import pytest
from unittest.mock import patch

from fabricengineer.logging.timer import TimeLogger, timer
from tests.utils import sniff_logs


def test_initial_state():
    """Test initial state of TimeLogger"""
    tl = TimeLogger()
    assert tl.start_time is None
    assert tl.end_time is None


def test_start_sets_start_time():
    """Test that start() sets the start time"""
    tl = TimeLogger()
    with patch('time.time', return_value=1234567890.0):
        result = tl.start()
        assert tl.start_time == 1234567890.0
        assert result is tl  # Test fluent interface


def test_stop_sets_end_time():
    """Test that stop() sets the end time"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    with patch('time.time', return_value=1234567891.0):
        result = tl.stop()
        assert tl.end_time == 1234567891.0
        assert result is tl  # Test fluent interface


def test_elapsed_time_calculation():
    """Test elapsed time calculation"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    tl._end_time = 1234567892.5
    assert tl.elapsed_time() == 2.5


def test_elapsed_time_rounding():
    """Test that elapsed time is rounded to 4 decimal places"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    tl._end_time = 1234567890.123456789
    assert tl.elapsed_time() == 0.1235


def test_elapsed_time_without_start_raises_error():
    """Test that elapsed_time raises error when not started"""
    tl = TimeLogger()
    with pytest.raises(ValueError, match="Timer has not been started and stopped properly"):
        tl.elapsed_time()


def test_elapsed_time_without_stop_raises_error():
    """Test that elapsed_time raises error when not stopped"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    with pytest.raises(ValueError, match="Timer has not been started and stopped properly"):
        tl.elapsed_time()


def test_log_start_message():
    """Test log output when only started"""
    tl = TimeLogger()
    with patch('time.time', return_value=1234567890.0):
        tl.start()

    _, logs = sniff_logs(lambda: tl.log())
    assert len(logs) == 1
    assert logs[0].startswith("TIMER-START:")
    assert "2009-02-" in logs[0]


def test_log_end_message():
    """Test log output when started and stopped"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    tl._end_time = 1234567892.5

    _, logs = sniff_logs(lambda: tl.log())
    assert len(logs) == 1
    assert logs[0].startswith("TIMER-END:")
    assert "2009-02-" in logs[0]
    assert "ELAPSED: 2.5s" in logs[0]


def test_log_invalid_state_message():
    """Test log output when timer is in invalid state"""
    tl = TimeLogger()
    # Timer not started
    _, logs = sniff_logs(lambda: tl.log())
    assert len(logs) == 1
    assert logs[0] == "Timer has not been started and stopped properly."


def test_str_representation():
    """Test string representation"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0
    tl._end_time = 1234567892.5

    str_repr = str(tl)
    assert "TimeLogger(" in str_repr
    assert "start_time=2009-02-" in str_repr
    assert "end_time=2009-02-" in str_repr
    assert "elapsed_time=2.5" in str_repr


def test_str_representation_without_end_time():
    """Test string representation when not stopped"""
    tl = TimeLogger()
    tl._start_time = 1234567890.0

    str_repr = str(tl)
    assert "elapsed_time=None" in str_repr


def test_repr_equals_str():
    """Test that repr equals str"""
    tl = TimeLogger()
    assert repr(tl) == str(tl)


def test_normal_usage_pattern():
    """Test the normal usage pattern described by user"""
    tl = TimeLogger()

    # Start timer and log
    _, start_logs = sniff_logs(lambda: tl.start().log())
    assert len(start_logs) == 1
    assert start_logs[0].startswith("TIMER-START:")

    # Simulate some work
    time.sleep(0.1)  # Use minimal sleep for test speed

    # Stop timer and log
    _, end_logs = sniff_logs(lambda: tl.stop().log())
    assert len(end_logs) == 1
    assert end_logs[0].startswith("TIMER-END:")
    assert "ELAPSED:" in end_logs[0]

    # Verify elapsed time is reasonable
    elapsed = tl.elapsed_time()
    assert 0.1 <= elapsed <= 0.2  # Should be around 0.1 seconds


def test_global_timer_instance():
    """Test that the global timer instance works"""
    # Reset global timer state
    timer._start_time = None
    timer._end_time = None

    assert timer.start_time is None
    assert timer.end_time is None

    # Test normal usage with global instance
    _, start_logs = sniff_logs(lambda: timer.start().log())
    assert len(start_logs) == 1
    assert start_logs[0].startswith("TIMER-START:")

    time.sleep(0.1)

    _, end_logs = sniff_logs(lambda: timer.stop().log())
    assert len(end_logs) == 1
    assert end_logs[0].startswith("TIMER-END:")


def test_multiple_start_calls():
    """Test that multiple start calls update the start time"""
    tl = TimeLogger()

    with patch('time.time', return_value=1000.0):
        tl.start()
        tl.stop()
        assert tl.start_time == 1000.0
        assert tl.end_time is not None

    with patch('time.time', return_value=2000.0):
        tl.start()
        assert tl.start_time == 2000.0
        assert tl.end_time is None


def test_stop_without_start():
    """Test stopping without starting"""
    tl = TimeLogger()

    with patch('time.time', return_value=1000.0):
        with pytest.raises(ValueError, match="Timer has not been started."):
            tl.stop()
