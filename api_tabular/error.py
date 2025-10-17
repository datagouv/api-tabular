# Backward compatibility imports from core module
from api_tabular.core.exceptions import QueryException, handle_exception

# Re-export for backward compatibility
__all__ = ["QueryException", "handle_exception"]
