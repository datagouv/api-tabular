import os

from pathlib import Path

import toml


class Configurator:
    """Loads a dict of config from TOML file(s) and behaves like an object, ie config.VALUE"""

    configuration = None

    def __init__(self):
        if not self.configuration:
            self.configure()

    def configure(self):
        # load default settings
        configuration = toml.load(Path(__file__).parent / "config_default.toml")

        # override with local settings
        local_settings = os.environ.get("CSVAPI_SETTINGS", Path.cwd() / "config.toml")
        if Path(local_settings).exists():
            configuration.update(toml.load(local_settings))

        # override with os env settings
        for config_key in configuration:
            if config_key in os.environ:
                configuration[config_key] = os.getenv(config_key)

        # Make sure PGREST_ENDPOINT has a scheme
        if not configuration["PGREST_ENDPOINT"].startswith("http"):
            configuration["PGREST_ENDPOINT"] = f"http://{configuration['PGREST_ENDPOINT']}"

        self.configuration = configuration
        self.check()

    def override(self, **kwargs):
        self.configuration.update(kwargs)
        self.check()

    def check(self):
        """Sanity check on config"""
        pass

    def __getattr__(self, __name):
        return self.configuration.get(__name)

    @property
    def __dict__(self):
        return self.configuration


config = Configurator()
