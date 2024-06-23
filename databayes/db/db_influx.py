"""
This file contains an InfluxDB data backend for the MOSAIC Project.
It defines the InfluxDB configuration and provides a concise implementation for
interacting with the InfluxDB, including data insertion, retrieval, update, and deletion.
"""

import pydantic
from tzlocal import get_localzone
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point
from influxdb_client.domain.organization import Organization
from .db_base import DBBase, DMBSConfigBase
import pandas as pd
import ast


def try_convert_to_dict(x):

    if isinstance(x, str) and x.startswith("{") and x.endswith("}"):
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError):
            # Return the original string if JSON decoding fails
            return x
    else:
        return x


class InfluxDBConfig(DMBSConfigBase):
    """
    Configuration class for connecting to an InfluxDB instance.

    Attributes:
        url (str): URL of the InfluxDB server.
        org (str): Organization for the InfluxDB server.
        token (str): Authorization token for the InfluxDB server.
    """

    url: str = pydantic.Field(default="", description="URL of InfluxDB")
    org: str = pydantic.Field(default=None, description="The Organization of InfluxDB")
    token: str = pydantic.Field(default="", description="InfluxDB authorization token")


class DBInfluxDB(DBBase):
    """
    InfluxDB backend that extends from DBBase and manages database operations for
    MOSAIC project's algorithmic trading.

    Attributes:
        config (InfluxDBConfig): An instance of the `InfluxDBConfig` class that holds
                                 the connection configuration details.
    """

    config: InfluxDBConfig = pydantic.Field(
        default=InfluxDBConfig(), description="The InfluxDB configuration"
    )

    def connect(self, **params):
        """
        Establishes a connection to an InfluxDB instance using parameters from the configuration.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        self.bkd = InfluxDBClient(
            url=self.config.url, token=self.config.token, org=self.config.org, **params
        )
        try:
            self.bkd.ping()
            if self.logger:
                self.logger.info("InfluxDB connected successfully")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to connect to InfluxDB: {str(e)}")

        return self

    def _resolve_bucket_measurement(self, endpoint):
        """
        Resolves the bucket and measurement from a given endpoint.

        Args:
            endpoint (str): The endpoint in the format `bucket/measurement`.

        Returns:
            tuple: A tuple containing bucket and measurement.
        """
        if self.name:
            bucket = self.name
            measurement = endpoint
        else:
            parts = endpoint.split("/")
            bucket = parts[0]
            measurement = parts[1] if len(parts) > 1 else None
        return bucket, measurement

    def _ensure_bucket_exists(self, bucket_name):
        """
        Checks if a bucket exists and creates it if it does not.

        Args:
            bucket_name (str): The name of the bucket to check and create if necessary.
        """
        bucket_api = self.bkd.buckets_api()
        found_bucket = bucket_api.find_bucket_by_name(bucket_name)
        if not found_bucket:
            if self.logger:
                self.logger.info(f"Creating bucket '{bucket_name}'.")
            bucket_api.create_bucket(bucket_name=bucket_name, org=self.config.org)

    def put(self, endpoint, data=[], index=[], time_field="_time", **params):
        """
        Inserts data into InfluxDB based on the specified endpoint and data.

        Args:
            endpoint (str): The endpoint defining where to store the data.
            data (list): List of data items to be inserted.
            index (list): List of indices.
            time_field (str): Field name for the time stamp.

        Returns:
            dict: A dictionary with success and failure counts.
        """
        bucket, measurement = self._resolve_bucket_measurement(endpoint)

        self._ensure_bucket_exists(bucket)

        points = []  # Pour stocker tous les points

        for item in data:
            point_measurement = item.get("measurement", measurement)
            point = Point(point_measurement)
            # # Ensure the timestamp is properly formatted
            # if time_field in item and isinstance(item[time_field],
            #                                      pd.Timestamp):
            #     point.time(item[time_field].isoformat())

            # Ajouter le timestamp s'il est présent dans la donnée
            if time_field in item:
                point.time(item[time_field])
                # Additionally, store the timestamp in a custom field with the original column name
                # point.field(time_field, item[time_field])

            for idx in index:  # tags
                if idx in item and idx != time_field:
                    value = item[idx]
                    if not isinstance(value, (str, int, float)):
                        idx = str(value)
                    point.tag(idx, value)

            # Traiter chaque clé/valeur dans item comme un champ,
            # sauf si c'est dans index, "measurement" ou "time_field"
            for key, value in item.items():
                if key not in index and key != "measurement" and key != time_field:
                    if not isinstance(value, (str, int, float)):
                        value = str(value)
                    point.field(key, value)

            points.append(point)  # Ajouter au tableau des points

        try:
            # Écrire tous les points en une seule fois
            with self.bkd.write_api(write_options=SYNCHRONOUS) as write_api:
                write_api.write(bucket=bucket, record=points)
            return {"success_count": len(data), "failure_count": 0}

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to put data in InfluxDB: {str(e)}")
            print(f"Erreur: {e}")
            return {"success_count": 0, "failure_count": len(data)}

    def get(
        self,
        endpoint,
        filter={},
        projection=[],
        limit=0,
        time_field="_time",
        as_df=False,
        use_local_tz=False,
        **params,
    ):
        """
        Retrieves data from InfluxDB based on query parameters.

        Args:
            endpoint (str): The endpoint to query from.
            filter (dict): Conditions to filter the data.
            projection (list): List of fields to project.
            limit (int): Maximum number of records to retrieve.
            keep_influx_id (bool): Whether to keep time field in results.
            time_field (str): Field name for the timestamp.

        Returns:
            list: List of records retrieved from InfluxDB.
        """
        bucket, measurement = self._resolve_bucket_measurement(endpoint)

        if self.logger:
            self.logger.debug(
                f"Retrieving data from bucket: {bucket}, measurement: {measurement}"
            )

        query = f'from(bucket: "{bucket}") |> range(start: 0)'
        if measurement:
            query += f' |> filter(fn: (r) => r._measurement == "{measurement}")'

        filter_conditions = [f'r["{k}"] == "{v}"' for k, v in filter.items()]
        if filter_conditions:
            query += f' |> filter(fn: (r) => {" and ".join(filter_conditions)})'

        if projection:
            fields_filter = " or ".join(
                [f'r._field == "{field}"' for field in projection]
            )
            query += f" |> filter(fn: (r) => {fields_filter})"

        if limit > 0:
            query += f" |> limit(n: {limit})"

        if self.logger:
            self.logger.debug(f"Constructed InfluxDB Query: {query}")

        #        try:

        result = self.bkd.query_api().query(query)

        data = []
        for table in result:
            for record in table.records:
                item = record.values
                data.append(item)

        df = pd.DataFrame(data)

        not_tag_fields = [
            "result",
            "table",
            "_start",
            "_stop",
            "_time",
            "_value",
            "_field",
            "_measurement",
        ]
        tag_fields = [var for var in df.columns if (var not in not_tag_fields)]

        # Convert to wide format by pivoting
        wide_df = df.pivot(
            index=tag_fields + ["_time"], columns="_field", values="_value"
        ).reset_index()
        wide_df.columns.name = None
        # Get the local timezone

        # Apply the local timezone to the '_time' column
        wide_df["_time"] = pd.to_datetime(wide_df["_time"])
        if use_local_tz:
            local_timezone = get_localzone()
            wide_df["_time"] = wide_df["_time"].dt.tz_convert(local_timezone)

        wide_df = wide_df.map(try_convert_to_dict)
        # Optionally rename '_time' index back to 'time_field'
        if time_field != "_time":
            wide_df.rename(columns={"_time": time_field}, inplace=True)

        if self.logger:
            self.logger.info(f"Retrieved {len(data)} records from InfluxDB.")

        # Returning the data in wide format
        return wide_df if as_df else wide_df.to_dict("records")

        # except Exception as e:
        #     if self.logger:
        #         self.logger.error(f"Failed to retrieve data from InfluxDB: {str(e)}")
        #     return []

    def update(self, endpoint, data, index=[], time_field="_time", **params):
        """
        Updates specific fields for data points matching the given tags.

        Args:
            endpoint (str): The endpoint to update.
            time_field (datetime): The timestamp to associate with the update.
            tags (dict): The tags identifying records to update.
            fields (dict): The fields to update with their new values.

        Returns:
            bool: True if the update is successful, False otherwise.
        """
        # # Formatage correct du timestamp pour InfluxDB
        # formatted_timestamp = time_field.strftime('%Y-%m-%dT%H:%M:%SZ')  # Format ISO 8601

        # # Construit la nouvelle donnée avec les champs mis à jour
        # data_to_update = []

        # # Créez un nouveau point avec les tags et les champs mis à jour
        # new_point = {
        #     "measurement": endpoint.split('/')[-1],
        #     "time_field": formatted_timestamp,
        #     **tags,  # Tags existants à conserver
        #     **fields  # Nouveaux champs à mettre à jour
        # }

        # # Ajoute la nouvelle pointe à la liste des données à mettre à jour
        # data_to_update.append(new_point)

        # Appelle la méthode put pour écrire la mise à jour dans InfluxDB
        result = self.put(
            endpoint=endpoint, data=[data], index=index, time_field=time_field, **params
        )

        # Vous pouvez vérifier le résultat et agir en conséquence
        if result["failure_count"] > 0:
            if self.logger:
                self.logger.error(f"Failed to update data in InfluxDB: {result}")
            return False
        else:
            if self.logger:
                self.logger.info(f"Data updated in InfluxDB: {result}")
            return True

    def reset(self, endpoint=None, **params):
        """
        Resets a bucket by deleting and recreating it.

        Args:
            endpoint (str): The endpoint associated with the bucket to reset.
        """
        if endpoint is None:
            bucket = self.name
        else:
            bucket, measurement = self._resolve_bucket_measurement(endpoint)

        # Suppression du bucket
        buckets_api = self.bkd.buckets_api()
        bucket_obj = buckets_api.find_bucket_by_name(bucket)
        if bucket_obj:
            try:
                buckets_api.delete_bucket(bucket_obj)
                if self.logger:
                    self.logger.info(f"Deleted bucket '{bucket}'.")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to delete bucket: {e}")
                raise e

        # # Recréation du bucket
        # try:
        #     buckets_api.create_bucket(bucket_name=bucket)
        #     if self.logger:
        #         self.logger.info(f"Recreated bucket '{bucket}'.")
        # except Exception as e:
        #     if self.logger:
        #         self.logger.error(f"Failed to recreate bucket: {e}")

    def size(self, endpoint):
        """
        Gets the size of data under the specified endpoint.

        Args:
            endpoint (str): The endpoint to measure size for.

        Returns:
            int: The number of records under the endpoint.
        """
        bucket, measurement = self._resolve_bucket_measurement(endpoint)

        query = f'from(bucket: "{bucket}") |> range(start: -1y) |> filter(fn: (r) => r["_measurement"] == "{measurement}") |> count(column: "_value")'
        tables = self.bkd.query_api().query(query, org=self.config.org)

        count = 0
        for table in tables:
            for record in table.records:
                count += record.get_value()

        return count

    def delete(self, endpoint, start_timestamp, stop_timestamp, tags):
        """
        Deletes data within a time range and matching the given tags.

        Args:
            endpoint (str): The endpoint associated with the data to delete.
            start_timestamp (datetime): Start timestamp for the deletion range.
            stop_timestamp (datetime): Stop timestamp for the deletion range.
            tags (dict): Tags that data to be deleted must match.

        Returns:
            bool: True if deletion is successful, False otherwise.
        """
        bucket, measurement = self._resolve_bucket_measurement(endpoint)

        delete_predicate = f'_measurement="{measurement}"'
        for tag_key, tag_value in tags.items():
            delete_predicate += f' AND {tag_key}="{tag_value}"'

        try:
            formatted_start = start_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            formatted_stop = stop_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            self.bkd.delete_api().delete(
                start=formatted_start,
                stop=formatted_stop,
                predicate=delete_predicate,
                bucket=bucket,
                org=self.config.org,
            )
            if self.logger:
                self.logger.info(
                    f"Deleted data in bucket '{bucket}' between '{formatted_start}' and '{formatted_stop}' with predicate '{delete_predicate}'."
                )
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to delete data: {e}")
            return False

    def close(self):
        """
        Closes the connection to the InfluxDB instance.
        """
        if hasattr(self, "bkd") and self.bkd:
            self.bkd.__del__()
            if self.logger:
                self.logger.info("InfluxDB connection closed.")

    def __enter__(self):
        """
        Enables use of the class in 'with' context for managing resources.

        Returns:
            self: The instance of `DBInfluxDB`.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensures the InfluxDB connection is closed when context is exited.
        """
        self.close()
