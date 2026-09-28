from pathlib import Path
import tomllib


def get_dev_config(
    connection_name: str = "default",
    connections_file_path: Path = Path.home() / ".snowflake/connections.toml",
) -> dict:
    with connections_file_path.open("rb") as config_file:
        config = tomllib.load(config_file)
    connections = config.get("connections", config)
    return dict(connections[connection_name])
