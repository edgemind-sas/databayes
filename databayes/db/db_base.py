import pydantic
import typing
import logging

from ..core import ObjCore

import pkg_resources

installed_pkg = {pkg.key for pkg in pkg_resources.working_set}
if "ipdb" in installed_pkg:
    import ipdb  # noqa: F401


class DBConfigBase(pydantic.BaseModel):
    """Base configuration model for database."""

    pass


class DMBSConfigBase(DBConfigBase):
    """
    Base configuration for database management systems (DBMS).

    Args:
        database: Name of the database.
        version: Version of the data backend provider.
        host: Host address of the database.
        port: Host port of the database.
        username: Username of the database user.
        password: Password of the database user.
    """

    database: str = pydantic.Field(None, description="DB database name")
    version: str = pydantic.Field(
        default=None, description="The data backend provider version"
    )
    host: str = pydantic.Field("localhost", description="DB host address")
    port: str = pydantic.Field(default=None, description="DB host port")
    username: str = pydantic.Field(default=None, description="DB user name")
    password: str = pydantic.Field(default=None, description="DB user password")


class DBBase(ObjCore):
    """
    Abstract data backend model.

    Args:
        name: Identifier of the data backend.
        config: Configuration of the data backend.
        logger: Logger for the database.
        bkd: Connector for the data backend.

    Methods:
        connect(**params):
            Establishes a connection to the database.

        count(endpoint, filter={}, **params):
            Counts number of records after filtering.

        update(endpoint, data=[], **params):
            Updates data in the database.

        put(endpoint, data={}, **params):
            Inserts data into the database.

        get(endpoint, filter={}, **params):
            Retrieves data from the database.

        delete(endpoint, filter={}, **params):
            Deletes data from the database.
    """

    name: str = pydantic.Field(None, description="Data backend id/name")
    config: DBConfigBase = pydantic.Field(
        default=DBConfigBase(), description="The data backend configuration"
    )
    logger: typing.Any = pydantic.Field(None, description="DB logger")
    bkd: typing.Any = pydantic.Field(
        default=None, description="The data backend connector"
    )

    def dict(self, **kwrds):

        attr_to_exclude = {"bkd", "logger"}

        if kwrds.get("exclude"):
            [kwrds["exclude"].add(attr) for attr in attr_to_exclude]
        else:
            kwrds["exclude"] = attr_to_exclude

        return super().dict(**kwrds)

    def connect(self, **params):
        """DB connection function."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def count(self, endpoint, filter={}, **params):
        """DB get data size after filtering."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def update(self, endpoint, data=[], **params):
        """DB update data function."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def put(self, endpoint, data={}, **params):
        """DB put data function."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def get(self, endpoint, filter={}, **params):
        """DB get data function."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def delete(self, endpoint, filter={}, **params):
        """DB delete data function."""
        raise NotImplementedError(
            "This function must be implemented in class {}".format(self.__class__)
        )

    def close(self):
        """
        Closes the connection to the DB instance. Must be overloaded if needed.
        """
        pass

    def __enter__(self):
        """
        Enables use of the class in 'with' context for managing resources.

        Returns:
            self: The instance of DB.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensures DB connection is closed when context is exited.
        """
        self.close()
