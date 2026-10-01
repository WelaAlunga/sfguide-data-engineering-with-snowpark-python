from snowflake.snowpark import Session
import os
from typing import Optional

# Class to store a singleton connection option
class SnowflakeConnection(object):
    _connection = None

    @property
    def connection(self) -> Optional[Session]:
        return type(self)._connection

    @connection.setter
    def connection(self, val):
        type(self)._connection = val

# Function to return a configured Snowpark session
def get_snowpark_session() -> Session:
    # if running in snowflake
    if SnowflakeConnection().connection:
        # Not sure what this does?
        session = SnowflakeConnection().connection
    # if running locally with a config file
    snowflake_config_path = os.path.expanduser('~/.snowflake/connections.toml')
    if os.path.exists(snowflake_config_path):
        snowpark_config = get_snowflake_config(config_file_path=snowflake_config_path)
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()
    # if using snowsql config, like snowcli does
    elif os.path.exists(os.path.expanduser('~/.snowsql/config')):
        snowpark_config = get_snowsql_config()
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()
    # otherwise configure from environment variables
    elif "SNOWSQL_ACCOUNT" in os.environ:
        snowpark_config = {
            "account": os.environ["SNOWSQL_ACCOUNT"],
            "user": os.environ["SNOWSQL_USER"],
            "password": os.environ["SNOWSQL_PWD"],
            "role": os.environ["SNOWSQL_ROLE"],
            "warehouse": os.environ["SNOWSQL_WAREHOUSE"],
            "database": os.environ["SNOWSQL_DATABASE"],
            "schema": os.environ["SNOWSQL_SCHEMA"]
        }
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()

    if SnowflakeConnection().connection:
        return SnowflakeConnection().connection  # type: ignore
    else:
        raise Exception("Unable to create a Snowpark session")


# Mimic the snowcli logic for getting config details, but skip the app.toml processing
# since this will be called outside the snowcli app context.
# TODO: It would be nice to get rid of this entirely and always use creds.json but
# need to update snowcli to make that happen
def get_snowsql_config(
    connection_name: str = 'dev',
    config_file_path: str = os.path.expanduser('~/.snowsql/config'),
) -> dict:
    import configparser

    snowsql_to_snowpark_config_mapping = {
        'account': 'account',
        'accountname': 'account',
        'username': 'user',
        'password': 'password',
        'rolename': 'role',
        'warehousename': 'warehouse',
        'dbname': 'database',
        'schemaname': 'schema'
    }
    try:
        config = configparser.RawConfigParser(inline_comment_prefixes="#")
        connection_path = 'connections.' + connection_name

        config.read(config_file_path)
        session_config = config[connection_path]
        # Convert snowsql connection variable names to snowcli ones
        session_config_dict = {
            snowsql_to_snowpark_config_mapping[k]: v.strip('"')
            for k, v in session_config.items()
        }
        return session_config_dict
    except Exception:
        raise Exception(
            "Error getting snowsql config details"
        )


def get_snowflake_config(
    connection_name: str = 'default',
    config_file_path: str = os.path.expanduser('~/.snowflake/connections.toml'),
) -> dict:
    import tomllib

    try:
        with open(config_file_path, "rb") as config_file:
            config = tomllib.load(config_file)
        connections = config.get('connections', config)
        if connection_name in connections:
            connection = connections[connection_name]
        elif len(connections) == 1:
            connection = next(iter(connections.values()))
        else:
            raise KeyError(
                f"Connection '{connection_name}' not found. "
                f"Available profiles: {', '.join(connections)}"
            )
        return {key: value for key, value in connection.items() if value}
    except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
        raise Exception(f"Error reading Snowflake connection profile: {exc}") from exc
