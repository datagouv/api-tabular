import os
import tomllib
from pathlib import Path


class Configurator:
    """Loads a dict of config from TOML file(s) and behaves like an object, ie config.VALUE"""

    configuration = None

    def __init__(self):
        if not self.configuration:
            self.configure()

    def configure(self):
        # load default settings
        with open(Path(__file__).parent / "config_default.toml", "rb") as f:
            configuration = tomllib.load(f)

        # override with local settings
        local_settings = os.environ.get("CSVAPI_SETTINGS", Path.cwd() / "config.toml")
        if Path(local_settings).exists():
            with open(local_settings, "rb") as f:
                configuration.update(tomllib.load(f))

        # override with os env settings
        for config_key in configuration:
            if config_key in os.environ:
                value = os.getenv(config_key)
                # Casting env value
                if isinstance(configuration[config_key], list):
                    value = value.split(",")
                elif isinstance(configuration[config_key], bool):
                    value = value.lower() in ["true", "1", "t", "y", "yes"]
                elif isinstance(configuration[config_key], int):
                    value = int(value)
                elif isinstance(configuration[config_key], float):
                    value = float(value)
                configuration[config_key] = value

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
