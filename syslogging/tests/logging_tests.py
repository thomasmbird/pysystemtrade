import pytest
from syslogging.logger import *


class TestLogging:
    def test_name(self, caplog):
        logger = get_logger("my_type")
        assert logger.name == "my_type"
        assert logger.getEffectiveLevel() == logging.DEBUG

        logger.info("foo %s", "bar")
        assert caplog.record_tuples == [("my_type", logging.INFO, "foo bar")]

    def test_attributes_good(self, caplog):
        logger = get_logger("my_type", {"stage": "bar"})
        logger.warn("foo %s", "bar")
        assert caplog.record_tuples == [
            ("my_type", logging.WARNING, "{'stage': 'bar'} foo bar")
        ]

    def test_attributes_bad(self):
        with pytest.raises(Exception):
            get_logger("my_type", {"foo": "bar"})

    def test_no_name_no_attributes(self, caplog):
        logger = get_logger("")
        logger.warn("foo")
        assert caplog.record_tuples == [("root", logging.WARNING, "foo")]

    def test_attributes_clear(self, caplog):
        clear = get_logger("Clear", {"stage": "first", "type": "one"})
        clear.info("Clearing attributes", method="clear")
        assert caplog.record_tuples == [("Clear", logging.INFO, "Clearing attributes")]
        caplog.clear()
        clear.info("Clearing attributes", method="clear", stage="second")
        assert caplog.record_tuples == [
            ("Clear", logging.INFO, "{'stage': 'second'} Clearing attributes")
        ]

    def test_attributes_preserve(self, caplog):
        preserve = get_logger("Preserve", {"stage": "first"})
        preserve.info(
            "Preserving attributes", method="preserve", type="one", stage="second"
        )
        assert caplog.record_tuples == [
            (
                "Preserve",
                logging.INFO,
                "{'type': 'one', 'stage': 'first'} Preserving attributes",
            )
        ]

    def test_attributes_overwrite(self, caplog):
        overwrite = get_logger("Overwrite", {"stage": "first"})
        overwrite.info(
            "Overwriting attributes", method="overwrite", type="one", stage="second"
        )
        assert caplog.record_tuples == [
            (
                "Overwrite",
                logging.INFO,
                "{'stage': 'second', 'type': 'one'} Overwriting attributes",
            )
        ]

    def test_setup(self):
        logger = get_logger("my_type", {"stage": "bar"})
        logger = logger.setup(stage="left")
        assert logger.name == "my_type"
        assert logger.extra["stage"] == "left"

        no_attrs = get_logger("no_attrs")
        no_attrs = no_attrs.setup(instrument_code="XYZ")
        assert no_attrs.extra["instrument_code"] == "XYZ"

    def test_setup_bad(self):
        logger = get_logger("my_type", {"stage": "bar"})
        with pytest.raises(Exception):
            logger.setup(foo="bar")

    def test_label(self):
        logger = get_logger("my_type", {"stage": "bar"})
        logger.label(stage="left", instrument_code="ABC")
        assert logger.name == "my_type"
        assert logger.extra["stage"] == "left"
        assert logger.extra["instrument_code"] == "ABC"

        no_attrs = get_logger("no_attrs")
        no_attrs.label(instrument_code="XYZ")
        assert no_attrs.extra["instrument_code"] == "XYZ"

    def test_label_bad(self):
        logger = get_logger("my_type", {"stage": "bar"})
        with pytest.raises(Exception):
            logger.label(stage="left", foo="bar")

    def test_msg(self, caplog):
        logger = get_logger("Msg")
        logger.msg("msg() is an alias for DEBUG")
        assert caplog.record_tuples == [
            ("Msg", logging.DEBUG, "msg() is an alias for DEBUG")
        ]

    def test_terse(self, caplog):
        logger = get_logger("Terse")
        logger.terse("terse() is an alias for INFO")
        assert caplog.record_tuples == [
            ("Terse", logging.INFO, "terse() is an alias for INFO")
        ]

    def test_warn(self, caplog):
        logger = get_logger("Warn")
        logger.warn("warn() is an alias for WARNING")
        assert caplog.record_tuples == [
            ("Warn", logging.WARNING, "warn() is an alias for WARNING")
        ]

    def test_logtoscreen(self):
        screen = logtoscreen("logtoscreen")
        assert screen.name == "logtoscreen"
        assert screen.getEffectiveLevel() == logging.DEBUG

        attrs = logtoscreen(**{TYPE_LOG_LABEL: "config", STAGE_LOG_LABEL: "config"})
        assert attrs.name == "config"
        assert attrs.getEffectiveLevel() == logging.DEBUG

    def test_set_logging_level(self):
        logger = get_logger("Set_Level")
        assert logger.getEffectiveLevel() == logging.DEBUG
        logger.set_logging_level(logging.INFO)
        assert logger.getEffectiveLevel() == 20

    def test_setup_empty_with(self):
        setup_with = get_logger("Setup_With", {"type": "foo", "stage": "one"})
        assert setup_with.extra["type"] == "foo"
        assert setup_with.extra["stage"] == "one"
        setup_with = setup_with.setup_empty_except_keep_type()
        assert setup_with.extra["type"] == "foo"
        assert "stage" not in setup_with.extra

    def test_setup_empty_without(self):
        setup_without = get_logger("Setup_Without", {"stage": "one"})
        setup_without = setup_without.setup_empty_except_keep_type()
        assert "type" not in setup_without.extra

        setup_without = get_logger("Setup_Without")
        setup_without = setup_without.setup_empty_except_keep_type()
        assert "type" not in setup_without.extra