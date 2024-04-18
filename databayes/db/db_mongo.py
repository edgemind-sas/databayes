import pymongo
import pydantic
import typing

from .db_base import DBBase, DMBSConfigBase

import pkg_resources


installed_pkg = {pkg.key for pkg in pkg_resources.working_set}
if 'ipdb' in installed_pkg:
    import ipdb  # noqa: F401

class DBMongo(DBBase):
    """
    The class `DBMongo` provides methods for interacting with a MongoDB database. Here's a summary of what each method does:

    - `connect`: Establishes a connection to the MongoDB server using the configuration provided.
    - `prepare_and_get_coll`: Prepares a collection in the database based on the given endpoint (e.g., database and collection names). Creates indexes if specified.
    - `size`: Returns the number of documents in a collection that match the given filter.
    - `get`: Retrieves documents from a collection that match the given filter and projection criteria.
    - `replace`: Replaces documents in a collection that match the given index with the provided data. If the documents don't exist, inserts them.
    - `update`: Updates documents in a collection that match the given index with the provided data. If the documents don't exist, inserts them.
    - `put`: Inserts documents into a collection. If the documents already exist, they are not inserted.
    - `reset`: Deletes all collections in the specified database.
    - `delete`: Deletes documents from a collection that match the given filter.
    - `log_db_ops`: Logs information about the performed database operations.
    """

    config: DMBSConfigBase = \
        pydantic.Field(default=DMBSConfigBase(),
                       description="The data backend configuration")

    def connect(self, serverSelectionTimeoutMS=2000, **params):

        self.bkd = pymongo.MongoClient(host=self.config.host,
                                       port=int(self.config.port),
                                       username=self.config.username,
                                       password=self.config.password,
                                       serverSelectionTimeoutMS=serverSelectionTimeoutMS,
                                       **params)

        bkd_server_info = self.bkd.server_info()
        connection_success = bkd_server_info.get('ok') == 1.0
        if self.logger:
            self.logger.debug(bkd_server_info)
            logger_info_msg = \
                f"Mongo DB {'connected' if connection_success else 'failed to connect'}"
            self.logger.info(logger_info_msg)
            
        return connection_success

    def prepare_and_get_coll(self, endpoint, index=[]):
        if self.name is None:
            db_name, coll_name = endpoint.split("/")
        else:
            db_name = self.name
            coll_name = endpoint

        db_coll = self.bkd[db_name][coll_name]
        if not (index is None):
            if not isinstance(index, (list, tuple, set)):
                index = [index]

            if len(index) > 0:
                db_coll.create_index([(idx, 1) for idx in index],
                                     unique=True)
        else:
            index = []

        return db_coll, index

    def size(self, endpoint,
             filter={},
             **params):

        db_coll, index = self.prepare_and_get_coll(endpoint)
        return db_coll.count_documents(filter=filter)

    def get(self, endpoint,
            filter={},
            projection=[],
            limit=0,
            keep_bkd_id=False,
            **params):

        db_coll, index = self.prepare_and_get_coll(endpoint)

        mongo_projection = {var: 1 for var in projection}
        mongo_projection.update({"_id": int(keep_bkd_id)})

        data_cs = db_coll.find(filter=filter,
                               projection=mongo_projection).limit(limit)

        return list(data_cs)

    def replace(self, endpoint,
                data=[],
                index=[],
                set_on_insert_data={},
                **params):

        db_coll, index = self.prepare_and_get_coll(endpoint, index)

        filter = {idx: data[idx] for idx in index}

        if isinstance(data, list):
            query_res_info = \
                [db_coll.replace_one(
                    filter,
                    dict(d, **set_on_insert_data),
                    upsert=True) for d in data]
        else:
            query_res_info = \
                [db_coll.replace_one(filter,
                                     dict(data, **set_on_insert_data),
                                     upsert=True)]

        if self.logger:
            # TODO: STOP USING THIS USE LOG_DB_OPS INSTEAD
            self.logger.debug(self.format_update_res(db_coll,
                                                     query_res_info))

    def update(self, endpoint,
               data=[],
               index=[],
               set_on_insert_data={},
               **params):

        res_dict = {"data_name": endpoint,
                    "ops_type": "update",
                    "ops": dict([
                        ("nb_data_processed", 0),
                        ("nb_inserts", 0),
                        ("nb_updates", 0)])}

        db_coll, index = self.prepare_and_get_coll(endpoint, index)

        set_query = {}
        if len(set_on_insert_data) > 0:
            set_query.update({"$setOnInsert": set_on_insert_data})

        try:
            if isinstance(data, list):
                ops_res_cur = \
                    [db_coll.update_one(
                        {idx: d[idx] for idx in index},
                        dict({"$set": d}, **set_query),
                        upsert=True) for d in data]

                res_dict["ops"]["nb_data_processed"] += len(data)
                res_dict["ops"]["nb_updates"] += \
                    sum([res.modified_count for res in ops_res_cur])
                res_dict["ops"]["nb_inserts"] += \
                    sum([res.upserted_id is not None for res in ops_res_cur])

            else:
                ops_res_cur = \
                    db_coll.update_one({idx: data[idx] for idx in index},
                                       dict({"$set": data}, **set_query),
                                       upsert=True)

                res_dict["ops"]["nb_data_processed"] += 1
                res_dict["ops"]["nb_updates"] += ops_res_cur.modified_count
                res_dict["ops"]["nb_inserts"] += ops_res_cur.upserted_id is not None

        except Exception as err:
            if self.logger:
                self.logger.error("Problem occurred updating data in endpoint {} : {}"
                                  .format(endpoint, err))

        self.log_db_ops(res_dict)

        return res_dict
        # if self.logger:
        #     # TODO: STOP USING THIS USE LOG_DB_OPS INSTEAD
        #     self.logger.debug(self.format_update_res(db_coll,
        #                                            update_res))

    def put(self, endpoint,
            data=[],
            index=[],
            **params):

        res_dict = {"data_name": endpoint,
                    "ops_type": "insert",
                    "ops": dict([
                        ("nb_data_processed", 0),
                        ("nb_inserts", 0),
                    ])}

        db_coll, index = self.prepare_and_get_coll(endpoint, index)

        # if has_index:
        #     # Drop duplicates based on index before inserting
        #     df.drop_duplicates(subset=data_index, inplace=True)
        try:
            if isinstance(data, list):
                if len(data) == 0:
                    return res_dict

                ops_res_cur = \
                    db_coll.insert_many(data, ordered=False)

                res_dict["ops"]["nb_data_processed"] = len(data)
                res_dict["ops"]["nb_inserts"] += len(ops_res_cur.inserted_ids)

            else:
                ops_res_cur = \
                    db_coll.insert_one(data)

                res_dict["ops"]["nb_data_processed"] += 1
                res_dict["ops"]["nb_inserts"] += ops_res_cur.acknowledged

        except Exception as err:
            if self.logger:
                self.logger.error("Problem occurred inserting data in endpoint {} : {}"
                                  .format(endpoint, err))

        self.log_db_ops(res_dict)

        return res_dict

    def reset(self, **params):
        if self.logger:
            self.logger.info(f"Reset DB {self.name}")
        for coll_name in self.bkd[self.name].list_collection_names():
            self.bkd[self.name][coll_name].drop()
            if self.logger:
                self.logger.debug(f"Drop collection {coll_name}")

    def delete(self, endpoint,
               filter={},
               **params):

        res_dict = {"data_name": endpoint,
                    "ops_type": "delete",
                    "ops": dict([
                        ("nb_deletions", 0)])}

        db_coll, index = self.prepare_and_get_coll(endpoint)

        ops_res_cur = db_coll.delete_many(filter=filter)

        res_dict["ops"]["nb_deletions"] = ops_res_cur.deleted_count

        self.log_db_ops(res_dict)

        return res_dict

    def log_db_ops(self, res_dict):
        log_msg_list = []

        data_name = res_dict.get("data_name")
        ops_type = res_dict.get("ops_type")
        etime = res_dict.get("etime")
        etime_str = f" - in {etime:.3} seconds" if etime else ""
        log_msg_ini = \
            f"Data source '{data_name}' - '{ops_type}' operation{etime_str}"

        log_msg_list.append(log_msg_ini)
        
        if self.logger:
            log_msg_list += \
                ["> {}: {}".format(ops, n)
                 for ops, n in res_dict["ops"].items()]
            log_msg = "\n".join(log_msg_list)
            self.logger.debug(log_msg)


    # TODO: STOP USING THIS USE LOG_DB_OPS INSTEAD
    def format_update_res(self, coll, res):
        if isinstance(res, list):
            update_keys = ["n", "nModified", "ok"]
            res_bis = {k: sum([r.raw_result[k] for r in res])
                       for k in update_keys}
            res_bis["upserted"] = [r.raw_result["upserted"]
                                   for r in res if "upserted" in r.raw_result.keys()]
            res_bis["nUpserted"] = len(res_bis["upserted"])
        else:
            res_bis = res.raw_result.copy()
            res_bis["nUpserted"] = 1 if "upserted" in res_bis.keys() else 0

        return "> collection {}: {:.0f} update request(s) correctly processed - items inserted: {:.0f} - items updated: {:.0f}" \
            .format(coll.name,
                    res_bis["ok"],
                    res_bis["nUpserted"],
                    res_bis["nModified"])
