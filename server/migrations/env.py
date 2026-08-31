import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlmodel import SQLModel

from alembic import context

# Importing clusterbuck.orm registers every migrated domain's table (jobs so far) on
# SQLModel.metadata — see clusterbuck/orm/__init__.py. Tables not yet migrated (the rest
# of _SCHEMA in clusterbuck/store.py) are deliberately absent from this metadata, so
# autogenerate must not be pointed at a live coordinator DB until they've all moved:
# it would otherwise propose dropping every unmigrated table.
import clusterbuck.orm  # noqa: F401,E402

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# CBK_DB_PATH matches clusterbuck/config.py's Settings.db_path — same env var the
# coordinator itself reads, so `alembic upgrade head` targets whatever DB `cbk-server`
# would open, rather than a path hardcoded in alembic.ini.
db_path = os.environ.get("CBK_DB_PATH", "cbk.db")
config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = SQLModel.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
