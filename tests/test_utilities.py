import configparser

from pytest import mark as m

from workbot.base import WorkBot
from workbot.utilities import is_builtins_module, parse_qualified_class_name, \
    qualified_class_name


@m.describe("Utilities")
class TestUtilities(object):

    def test_is_builtins_module(self):
        assert is_builtins_module(str.__class__.__module__)
        assert not is_builtins_module(configparser.ConfigParser.__module__)

    def test_parse_qualified_class_name(self):
        name = "workbot.base.WorkBot"
        module_name, class_name = parse_qualified_class_name(name)
        assert module_name == "workbot.base"
        assert class_name == "WorkBot"

    def test_qualified_class_name(self):
        name = qualified_class_name(WorkBot)
        assert name == "workbot.base.WorkBot"
