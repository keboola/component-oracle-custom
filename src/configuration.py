import dataclasses
import json
from dataclasses import dataclass, field
from typing import List, Optional

import dataconf
from keboola.utils.helpers import comma_separated_values_to_list


class ConfigurationBase:

    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class DbOptions(ConfigurationBase):
    host_port: str
    database: str
    user: str
    pswd_password: str

    @property
    def host(self) -> str:
        return self.host_port.split(':')[0]

    @property
    def port(self) -> int:
        return int(self.host_port.split(':')[1])


@dataclass
class Script(ConfigurationBase):
    continue_on_failure: bool
    script: str


@dataclass
class LoadingOptions(ConfigurationBase):
    load_type: str
    full_load_mode: Optional[str] = 'truncate_as_delete'
    incremental_load_mode: Optional[str] = 'sql_loader'
    full_load_procedure: Optional[str] = None
    full_load_procedure_parameters: Optional[str] = None
    mode: Optional[str] = None

    @property
    def full_load_procedure_parameters_list(self):
        if self.full_load_procedure_parameters:
            return comma_separated_values_to_list(self.full_load_procedure_parameters)
        else:
            return None


@dataclass
class SQLLoaderOptions(ConfigurationBase):
    rows: int = 5000
    bindsize: int = 8000000
    readsize: int = 8000001


@dataclass
class DefaultFormatOptions(ConfigurationBase):
    date_format: str = 'YYYY-MM-DD'
    timestamp_format: str = "YYYY-MM-DD HH24:MI:SS.FF6"


@dataclass
class ColumnMapping(ConfigurationBase):
    source_name: str
    destination_name: str


@dataclass
class Configuration(ConfigurationBase):
    # Connection options
    db: DbOptions
    schema: str
    table_name: str
    loading_options: LoadingOptions
    default_format_options: DefaultFormatOptions
    sql_loader_options: Optional[SQLLoaderOptions] = None
    post_run_script: bool = False
    post_run_scripts: Optional[Script] = None
    pre_run_script: bool = False
    pre_run_scripts: Optional[Script] = None
    custom_column_mapping: bool = False
    columns: List[ColumnMapping] = field(default_factory=list)
    debug: bool = False

    def __post_init__(self):
        if not self.sql_loader_options:
            self.sql_loader_options = SQLLoaderOptions()
