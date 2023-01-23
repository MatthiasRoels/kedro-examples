# import logging
from copy import deepcopy
from glob import iglob
from pathlib import Path
from typing import Any, Dict, List

from omegaconf import OmegaConf, DictConfig
from kedro.config import AbstractConfigLoader, MissingConfigException
from yaml.parser import ParserError
from yaml.scanner import ScannerError

# _config_logger = logging.getLogger(__name__)


class ConfigLoader(AbstractConfigLoader):
    """Recursively scan directories (config paths) contained in ``conf_source`` for
    configuration files with a ``yaml``, ``yml`` or ``json`` extension, load and merge
    them through ``OmegaConf`` (https://omegaconf.readthedocs.io/)
    and return them in the form of a config dictionary.
    The first processed config path is the ``base`` directory inside
    ``conf_source``. The optional ``env`` argument can be used to specify a
    subdirectory of ``conf_source`` to process as a config path after ``base``.
    When the same top-level key appears in any two config files located in
    the same (sub)directory, a ``ValueError`` is raised.
    When the same key appears in any two config files located in different
    (sub)directories, the last processed config path takes precedence
    and overrides this key and any sub-keys.
    You can access the different configurations as follows:
    ::
        >>> import logging.config
        >>> from kedro.framework.project import settings
        >>> from prototype.lib import ConfigLoader
        >>>
        >>> conf_path = str(project_path / settings.CONF_SOURCE)
        >>> conf_loader = ConfigLoader(conf_source=conf_path, env="local")
        >>>
        >>> conf_logging = conf_loader["logging"]
        >>> logging.config.dictConfig(conf_logging)  # set logging conf
        >>>
        >>> conf_catalog = conf_loader["catalog"]
        >>> conf_params = conf_loader["parameters"]
    ``OmegaConf`` supports variable interpolation in configuration
    https://omegaconf.readthedocs.io/en/2.2_branch/usage.html#merging-configurations. It is
    recommended to use this instead of yaml anchors with the ``OmegaConfLoader``.
    To use this class, change the setting for the `CONFIG_LOADER_CLASS` constant
    in `settings.py`.
    Example:
    ::
        >>> # in settings.py
        >>> from prototype.lib import ConfigLoader
        >>>
        >>> CONFIG_LOADER_CLASS = ConfigLoader
    """

    def __init__(
        self,
        conf_source: str,
        env: str = None,
        runtime_params: Dict[str, Any] = None,
        *,
        config_patterns: Dict[str, List[str]] = None,
        base_env: str = "base",
        default_run_env: str = "local",
    ):
        """Instantiates a ``OmegaConfLoader``.
        Args:
            conf_source: Path to use as root directory for loading configuration.
            env: Environment that will take precedence over base.
            runtime_params: Extra parameters passed to a Kedro run.
            config_patterns: Regex patterns that specify the naming convention for configuration
                files so they can be loaded. Can be customised by supplying config_patterns as
                in `CONFIG_LOADER_ARGS` in `settings.py`.
            base_env: Name of the base environment. Defaults to `"base"`.
                This is used in the `conf_paths` property method to construct
                the configuration paths.
            default_run_env: Name of the default run environment. Defaults to `"local"`.
                Can be overridden by supplying the `env` argument.
        """
        self.base_env = base_env
        self.default_run_env = default_run_env

        self.config_patterns = {
            "catalog": ["catalog*", "catalog*/**", "**/catalog*"],
            "parameters": ["parameters*", "parameters*/**", "**/parameters*"],
            "credentials": [
                "credentials*",
                "credentials*/**",
                "**/credentials*",
            ],
            "logging": ["logging*", "logging*/**", "**/logging*"],
        }
        self.config_patterns.update(config_patterns or {})

        super().__init__(
            conf_source=conf_source,
            env=env,
            runtime_params=runtime_params,
        )

    def __repr__(self):  # pragma: no cover
        return (
            f"ConfigLoader(conf_source={self.conf_source}, env={self.env}, "
            f"config_patterns={self.config_patterns})"
        )

    @property
    def conf_paths(self):
        """Property method to return deduplicated configuration paths."""
        return self._build_conf_paths()

    def __getitem__(self, key) -> Dict[str, Any]:
        """Get configuration files by key, load and merge them, and
        return them in the form of a config dictionary.
        Args:
            key: Key of the configuration type to fetch.
        Raises:
            KeyError: If key provided isn't present in the config_patterns of
                this OmegaConfLoader instance.
            MissingConfigException: If no configuration files exist matching the patterns
                mapped to the provided key
        Returns:
            Dict[str, Any]:  A Python dictionary with the combined
               configuration from all configuration files.
        """
        # Allow bypassing of loading config from patterns if a key and value have been set
        # explicitly on the ``ConfigLoader`` instance.
        if key in self:
            return super().__getitem__(key)

        if key not in self.config_patterns:
            raise KeyError(
                f"No config patterns were found for '{key}' in your config loader"
            )

        loaded_configs = [
            self._load_and_merge_dir_config(
                conf_path, self.config_patterns[key]
            )
            for conf_path in self.conf_paths
        ]

        merge_strategy = self._get_merge_strategy(key)
        merged_conf = self._merge_configs(loaded_configs, merge_strategy)

        # cast to dict and return
        config = OmegaConf.to_container(merged_conf, resolve=True)

        if not config:
            run_env = self.env or self.default_run_env
            raise MissingConfigException(
                f"No files of YAML or JSON format found in {self.base_env} or {run_env} matching"
                f" the glob pattern(s): {[*self.config_patterns[key]]}"
            )

        return config

    def _build_conf_paths(self) -> List[str]:

        run_env = self.env or self.default_run_env

        # to ensure we return deduplicated configuration paths
        if run_env == self.base_env:
            return [str(Path(self.conf_source) / self.base_env)]

        return [
            str(Path(self.conf_source) / self.base_env),
            str(Path(self.conf_source) / run_env),
        ]

    def _get_merge_strategy(self, conf_type: str) -> str:
        """Get merge strategy to merge DictConf objects.

        Args:
            conf_type (str): type of configuration (e.g. parameters)

        Returns:
            str: merge strategy for configuration. Is either merge or overwrite.
        """
        # only use OmegaConf merge with parameters. Hardcoded for now until
        # we found a way to nicely expose this
        merge_strategy = "overwrite"
        if conf_type == "parameters":
            merge_strategy = "merge"

        return merge_strategy

    @staticmethod
    def _load_and_merge_dir_config(
        conf_path: str,
        patterns: List[str],
    ) -> DictConfig:
        """Recursively load and merge all configuration files in a directory using OmegaConf,
        which satisfy a given list of glob patterns from a specific path.
        Args:
            conf_path: Path to configuration directory.
            patterns: List of glob patterns to match the filenames against.
        Raises:
            MissingConfigException: If configuration path doesn't exist or isn't valid.
            ValueError: If two or more configuration files contain the same key(s).
            ParserError: If config file contains invalid YAML or JSON syntax.
        Returns:
            Resulting configuration dictionary.
        """
        if not Path(conf_path).is_dir():
            raise MissingConfigException(
                f"Given configuration path either does not exist "
                f"or is not a valid directory: {conf_path}"
            )

        # `Path.glob()` ignores the files if pattern ends with "**",
        # therefore iglob is used instead
        paths = [
            Path(each).resolve()
            for pattern in patterns
            for each in iglob(f"{str(conf_path)}/{pattern}", recursive=True)
        ]

        config_files_filtered = [
            path
            for path in set(paths)  # use set to remove duplicate filepaths
            if path.is_file() and path.suffix in [".yml", ".yaml", ".json"]
        ]

        conf_by_path = {}
        for config_filepath in config_files_filtered:
            try:
                conf_by_path[config_filepath] = OmegaConf.load(config_filepath)
            except (ParserError, ScannerError) as exc:
                line = exc.problem_mark.line  # type: ignore
                cursor = exc.problem_mark.column  # type: ignore
                raise ParserError(
                    f"Invalid YAML or JSON file {config_filepath}, unable to read line {line}, "
                    f"position {cursor}."
                ) from exc

        # Check for duplicate top-level keys in the configs
        # method raises ValueError if duplicates are found
        ConfigLoader._check_duplicate_top_level_keys(conf_by_path)

        aggregate_config = list(conf_by_path.values())
        if not aggregate_config:
            return OmegaConf.create({})

        # merge OmegaConf objects. This is safe since there are no
        # duplicate top-level keys
        return OmegaConf.merge(*aggregate_config)

    @staticmethod
    def _merge_configs(
        loaded_configs: List[DictConfig],
        merge_strategy: str = "overwrite",
    ) -> DictConfig:
        """Merge a list of loaded configurations where the index refers to the
        priority of the configuration path

        Args:
            loaded_configs: ordered list of loaded configurations.
                Each entry in the list contains the configuration for a
                particular configuration path. The index of the entry refers to
                the priority of the configuration path (e.g. the first entry
                can be overwritten by the second entry etc).
            merge_strategy: Value should be one of "overwrite" or "merge".
                Determines whether top-level keys of configurations from
                different envs should be overwritten or merged.
                Defaults to "overwrite".

        Returns:
            A DictConfig the combined configuration from all
            configuration files.
        """
        if len(loaded_configs) == 1:
            return loaded_configs[0]

        if merge_strategy == "merge":
            return OmegaConf.merge(*loaded_configs)

        # Destructively merge the configurations. We can do this using the update
        # method of the dict class. Hence we first convert DictConfif objects to
        # dicts, use the dict.update method and convert back to a DictConfig
        # we set resolve to False as we only do this in the final step!
        final = OmegaConf.to_container(loaded_configs[0])
        for overwrite_conf in loaded_configs[1:]:
            overwrite_dict = OmegaConf.to_container(
                overwrite_conf, resolve=False
            )
            final.update(overwrite_dict)

        return OmegaConf.create(final)

    @staticmethod
    def _check_duplicate_top_level_keys(
        conf_by_path: dict[str, DictConfig]
    ) -> None:

        top_level_keys_by_path = {
            file: set(conf.keys()) for file, conf in conf_by_path.items()
        }

        duplicates = []

        filepaths = list(top_level_keys_by_path.keys())
        for i, filepath1 in enumerate(filepaths, 1):
            config1 = top_level_keys_by_path[filepath1]
            for filepath2 in filepaths[i:]:
                config2 = top_level_keys_by_path[filepath2]

                overlapping_keys = config1 & config2

                if overlapping_keys:
                    sorted_keys = ", ".join(sorted(overlapping_keys))
                    if len(sorted_keys) > 100:
                        sorted_keys = sorted_keys[:100] + "..."
                    duplicates.append(
                        f"Duplicate keys found in {filepath1} and {filepath2}: {sorted_keys}"
                    )

        if duplicates:
            dup_str = "\n".join(duplicates)
            raise ValueError(f"{dup_str}")
