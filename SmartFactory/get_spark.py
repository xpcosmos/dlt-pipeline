from databricks.connect import DatabricksSession

# This automatically picks up your CLI authentication
spark = DatabricksSession.builder.getOrCreate()

__all__ = ["spark"]