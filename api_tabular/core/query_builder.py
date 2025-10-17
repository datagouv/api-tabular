"""
Query building logic for the core module.

This module contains the query building functionality extracted from utils.py.
It handles SQL query construction, filtering, sorting, and aggregation.
"""

from collections import defaultdict

from .. import config


class QueryBuilder:
    """Handles SQL query building for PostgREST."""

    def __init__(self):
        self.TYPE_POSSIBILITIES = {
            "string": ["compare", "contains", "differs", "exact", "in", "sort", "groupby", "count"],
            "float": [
                "compare",
                "differs",
                "exact",
                "in",
                "sort",
                "groupby",
                "count",
                "avg",
                "max",
                "min",
                "sum",
            ],
            "int": [
                "compare",
                "differs",
                "exact",
                "in",
                "sort",
                "groupby",
                "count",
                "avg",
                "max",
                "min",
                "sum",
            ],
            "bool": ["differs", "exact", "in", "sort", "groupby", "count"],
            "date": ["compare", "contains", "differs", "exact", "in", "sort", "groupby", "count"],
            "datetime": [
                "compare",
                "contains",
                "differs",
                "exact",
                "in",
                "sort",
                "groupby",
                "count",
            ],
            "json": ["contains", "differs", "exact", "in", "groupby", "count"],
        }

        self.MAP_TYPES = {
            # defaults to "string"
            "bool": "boolean",
            "int": "integer",
            "float": "number",
        }

        self.OPERATORS_DESCRIPTIONS = {
            "exact": {
                "name": "{}__exact",
                "description": "Exact match in column: {} ({}__exact=value)",
            },
            "differs": {
                "name": "{}__differs",
                "description": "Differs from in column: {} ({}__differs=value)",
            },
            "contains": {
                "name": "{}__contains",
                "description": "String contains in column: {} ({}__contains=value)",
            },
            "in": {
                "name": "{}__in",
                "description": "Value in list in column: {} ({}__in=value1,value2,...)",
            },
            "groupby": {
                "name": "{}__groupby",
                "description": "Performs `group by values` operation in column: {}",
                "is_aggregator": True,
            },
            "count": {
                "name": "{}__count",
                "description": "Performs `count values` operation in column: {}",
                "is_aggregator": True,
            },
            "avg": {
                "name": "{}__avg",
                "description": "Performs `mean` operation in column: {}",
                "is_aggregator": True,
            },
            "min": {
                "name": "{}__min",
                "description": "Performs `minimum` operation in column: {}",
                "is_aggregator": True,
            },
            "max": {
                "name": "{}__max",
                "description": "Performs `maximum` operation in column: {}",
                "is_aggregator": True,
            },
            "sum": {
                "name": "{}__sum",
                "description": "Performs `sum` operation in column: {}",
                "is_aggregator": True,
            },
        }

    def is_aggregation_allowed(self, resource_id: str) -> bool:
        """Check if aggregation is allowed for a resource."""
        return resource_id in config.ALLOW_AGGREGATION

    def build_sql_query_string(
        self,
        request_arg: list,
        resource_id: str | None = None,
        indexes: set | None = None,
        page_size: int = None,
        offset: int = 0,
    ) -> str:
        """
        Build SQL query string from request arguments.

        Args:
            request_arg: List of request arguments
            resource_id: Resource ID for aggregation checks
            indexes: Set of allowed column indexes
            page_size: Number of records per page
            offset: Number of records to skip

        Returns:
            SQL query string for PostgREST
        """
        sql_query = []
        aggregators = defaultdict(list)
        sorted = False
        for arg in request_arg:
            _split = arg.split("=")
            # filters are expected to have the syntax `<column_name>__<operator>=<value>`
            if len(_split) == 2:
                _filter, _sorted = self.add_filter(*_split, indexes)
                if _filter:
                    sorted = sorted or _sorted
                    sql_query.append(_filter)
            # aggregators are expected to have the syntax `<column_name>__<operator>`
            elif len(_split) == 1:
                column, operator = self.add_aggregator(_split[0], indexes)
                if column:
                    aggregators[operator].append(column)
            else:
                raise ValueError(f"argument '{arg}' could not be parsed")
        if aggregators:
            if resource_id and not self.is_aggregation_allowed(resource_id):
                raise PermissionError(
                    f"Aggregation parameters `{'`, `'.join(aggregators.keys())}` "
                    f"are not allowed for resource '{resource_id}'"
                )
            agg_query = "select="
            for operator in aggregators:
                if operator == "groupby":
                    agg_query += f"{','.join(aggregators[operator])},"
                else:
                    for column in aggregators[operator]:
                        # aggregated columns are named `<column_name>__<operator>`
                        # we pop the heading and trailing " that were added upstream
                        # and put them around the new column name
                        agg_query += f'"{column[1:-1]}__{operator}":{column}.{operator}(),'
            # we pop the trailing comma (it's always there, by construction)
            sql_query.append(agg_query[:-1])
        if page_size:
            sql_query.append(f"limit={page_size}")
        if offset >= 1:
            sql_query.append(f"offset={offset}")
        if not sorted and not aggregators:
            sql_query.append("order=__id.asc")
        q = "&".join(sql_query)
        if q.count("select=") > 1:
            raise ValueError("the argument `columns` cannot be set alongside aggregators")
        return q

    def get_column_and_operator(self, argument: str) -> tuple[str, str]:
        """Extract column name and operator from argument."""
        *column_split, comparator = argument.split("__")
        normalized_comparator = comparator.lower()
        # handling headers with "__" and special characters
        # we're escaping the " because they are the encapsulators of the label
        column = '"{}"'.format("__".join(column_split).replace('"', '\\"'))
        return column, normalized_comparator

    def add_filter(self, argument: str, value: str, indexes: set | None) -> tuple[str | None, bool]:
        """Add a filter to the query."""
        if argument in ["page", "page_size"]:  # processed differently
            return None, False
        if argument == "columns":
            return f"select={value}", False
        if "__" in argument:
            column, normalized_comparator = self.get_column_and_operator(argument)
            self.raise_if_not_index(column, indexes)
            if normalized_comparator == "sort":
                return f"order={column}.{value}", True
            elif normalized_comparator == "exact":
                return f"{column}=eq.{value}", False
            elif normalized_comparator == "differs":
                return f"{column}=neq.{value}", False
            elif normalized_comparator == "contains":
                return f"{column}=ilike.*{value}*", False
            elif normalized_comparator == "in":
                return f"{column}=in.({value})", False
            elif normalized_comparator == "less":
                return f"{column}=lte.{value}", False
            elif normalized_comparator == "greater":
                return f"{column}=gte.{value}", False
            elif normalized_comparator == "strictly_less":
                return f"{column}=lt.{value}", False
            elif normalized_comparator == "strictly_greater":
                return f"{column}=gt.{value}", False
        raise ValueError(f"argument '{argument}={value}' could not be parsed")

    def add_aggregator(self, argument: str, indexes: set | None) -> tuple[str, str]:
        """Add an aggregator to the query."""
        operator = None
        if "__" in argument:
            column, operator = self.get_column_and_operator(argument)
            self.raise_if_not_index(column, indexes)
        if operator in ["avg", "count", "max", "min", "sum", "groupby"]:
            return column, operator
        raise ValueError(f"argument '{argument}' could not be parsed")

    def raise_if_not_index(self, column_name: str, indexes: set | None) -> None:
        """Raise error if column is not in allowed indexes."""
        if indexes is None:
            return
        # we pop the heading and trailing " that were added upstream
        if column_name[1:-1] not in indexes:
            raise PermissionError(
                f"{column_name[1:-1]} is not among the allowed columns: {indexes}"
            )
