import pydantic
import pkg_resources
import copy
import typing
import yaml


installed_pkg = {pkg.key for pkg in pkg_resources.working_set}
if "ipdb" in installed_pkg:
    import ipdb  # noqa: F401


class ObjCore(pydantic.BaseModel):

    @classmethod
    def get_subclasses(cls, recursive=True):
        """Enumerates all subclasses of a given class.

        # Arguments
        cls: class. The class to enumerate subclasses for.
        recursive: bool (default: True). If True, recursively finds all sub-classes.

        # Return value
        A list of subclasses of `cls`.
        """
        sub = cls.__subclasses__()
        if recursive:
            for cls in sub:
                sub.extend(cls.get_subclasses(recursive))
        return sub

    @classmethod
    def from_yaml(
        cls,
        file_path: str,
        add_cls=True,
        attr_header=None,
        cls_attr=None,
    ):
        with open(file_path, "r", encoding="utf-8") as yaml_file:
            obj_dict = yaml.load(yaml_file, Loader=yaml.SafeLoader)
            if attr_header:
                obj_dict = obj_dict[attr_header]
            if add_cls:
                obj_dict.setdefault("cls", cls.__name__)
            if cls_attr:
                obj_dict["cls"] = cls_attr
            return cls.from_dict(obj_dict)

    @classmethod
    def from_dict(basecls, obj):

        # ipdb.set_trace()
        if isinstance(obj, dict):
            obj_copy = copy.deepcopy(obj)
            for key, value in obj_copy.items():
                obj_copy[key] = basecls.from_dict(value)

            if "cls" in obj_copy:
                cls_sub_dict = {cls.__name__: cls for cls in ObjMOSAIC.get_subclasses()}

                clsname = obj_copy.pop("cls")
                cls = cls_sub_dict.get(clsname)

                if cls is None:
                    raise ValueError(
                        f"{clsname} is not a subclass of {ObjMOSAIC.__name__}"
                    )

                return cls(**obj_copy)

        elif isinstance(obj, list):
            for index, value in enumerate(obj):
                obj[index] = basecls.from_dict(value)

        return obj

    def update(self, **new_data):
        if len(new_data) > 0:
            for field, value in new_data.items():
                setattr(self, field, value)

    def dict(self, **kwrds):
        return dict({"cls": self.__class__.__name__}, **super().dict(**kwrds))
