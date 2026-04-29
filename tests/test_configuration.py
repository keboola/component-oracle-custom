import unittest

from configuration import Configuration, Script


def _base_config(**overrides):
    cfg = {
        "db": {
            "host_port": "localhost:1521",
            "database": "ORCL",
            "user": "u",
            "pswd_password": "p",
        },
        "schema": "S",
        "table_name": "T",
        "loading_options": {"load_type": "full_load"},
        "default_format_options": {},
    }
    cfg.update(overrides)
    return cfg


class TestScriptConfiguration(unittest.TestCase):
    def test_partial_post_run_scripts_loads(self):
        """Regression: a stale post_run_scripts object missing the script field
        must not break config parsing."""
        cfg = _base_config(post_run_scripts={"continue_on_failure": False})
        parsed = Configuration.load_from_dict(cfg)
        self.assertIsInstance(parsed.post_run_scripts, Script)
        self.assertIsNone(parsed.post_run_scripts.script)

    def test_partial_pre_run_scripts_loads(self):
        cfg = _base_config(pre_run_scripts={"continue_on_failure": True})
        parsed = Configuration.load_from_dict(cfg)
        self.assertIsInstance(parsed.pre_run_scripts, Script)
        self.assertIsNone(parsed.pre_run_scripts.script)

    def test_empty_scripts_object_loads(self):
        cfg = _base_config(post_run_scripts={}, pre_run_scripts={})
        parsed = Configuration.load_from_dict(cfg)
        self.assertIsInstance(parsed.post_run_scripts, Script)
        self.assertIsInstance(parsed.pre_run_scripts, Script)

    def test_fully_specified_script_loads(self):
        cfg = _base_config(
            post_run_script=True,
            post_run_scripts={"continue_on_failure": True, "script": "SELECT 1"},
        )
        parsed = Configuration.load_from_dict(cfg)
        self.assertEqual(parsed.post_run_scripts.script, "SELECT 1")
        self.assertTrue(parsed.post_run_scripts.continue_on_failure)


if __name__ == "__main__":
    unittest.main()
