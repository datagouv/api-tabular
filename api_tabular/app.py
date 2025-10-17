# Backward compatibility - re-export from API module
from api_tabular.api.app import app_factory, run

# Keep the old imports for backward compatibility
from api_tabular.query import (
    get_potential_indexes,
    get_resource,
    get_resource_data,
    get_resource_data_streamed,
)
from api_tabular.utils import (
    build_link_with_page,
    build_sql_query_string,
    build_swagger_file,
    get_app_version,
    url_for,
)

# All route definitions and app factory are now in api_tabular.api.app


if __name__ == "__main__":
    run()
