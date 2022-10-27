from dataclasses import dataclass, field
from typing import List, Dict, Callable
from typing import Optional


@dataclass
class ColumnSchema:
    """
    Defines the name and type specifications of a single field in a table
    """
    name: str
    source_type: Optional[str] = None
    source_type_signature: Optional[str] = None
    base_type_converter: Callable[[str | None], str] = field(default=lambda s: s)
    description: Optional[str] = None
    nullable: bool = False
    length: Optional[str] = None
    default: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def base_type(self):
        return self.base_type_converter(self.source_type)


@dataclass
class TableSchema:
    """
    TableSchema class is used to define the schema and metadata of a table.
    """
    name: str
    columns: List[ColumnSchema]
    primary_keys: Optional[List[str]] = None
    parent_tables: Optional[List[str]] = None
    description: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def field_names(self) -> List[str]:
        return [column.name for column in self.columns]

    @property
    def csv_name(self) -> str:
        return f"{self.name}.csv"

    def add_column(self, column: ColumnSchema) -> None:
        """
        Adds extra field to the tableschema.
        Args:
            column:  ColumnSchema to add to the list of columns

        """
        self.columns.append(column)


def init_table_schema_from_dict(json_table_schema: Dict,
                                base_type_converter: Callable[[str | None], str] = field(
                                    default=lambda s: s)) -> TableSchema:
    """
    Function to initialize a Table Schema from a dictionary.
    Example of the json_table_schema structure:
    {
      "name": "product",
      "description": "this table holds data on products",
      "parent_tables": [],
      "primary_keys": [
        "id"
      ],
      "columns": [
        {
          "name": "id",
          "base_type": "string",
          "description": "ID of the product",
          "length": "100",
          "nullable": false
        },
        {
          "name": "name",
          "base_type": "string",
          "description": "Plain-text name of the product",
          "length": "1000",
          "default": "Default Name"
        }
      ]
    }
    """
    try:
        json_table_schema["columns"] = [ColumnSchema(**{**_field, **{"base_type_converter": base_type_converter}}) for
                                        _field in json_table_schema["columns"]]
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of columns failed : {type_error}") from type_error
    try:
        ts = TableSchema(**json_table_schema)
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of the table failed : {type_error}") from type_error
    return ts
