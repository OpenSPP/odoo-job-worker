import json
from datetime import date, datetime

from odoo import models

_SAFE_CONTEXT_KEYS = {
    "lang",
    "tz",
    "active_test",
    "allowed_company_ids",
    "company_id",
    "force_company",
}


class JobEncoder(json.JSONEncoder):
    """
    Custom JSONEncoder for Odoo records and types.
    """

    def default(self, obj):
        if isinstance(obj, models.BaseModel):
            safe_context = {
                key: obj.env.context.get(key)
                for key in _SAFE_CONTEXT_KEYS
                if key in obj.env.context
            }
            return {
                "__type__": "odoo_recordset",
                "model": obj._name,
                "ids": obj.ids,
                "uid": obj.env.uid,
                "su": obj.env.su,
                "context": safe_context,
            }
        if isinstance(obj, datetime):
            return {"__type__": "datetime_isoformat", "value": obj.isoformat()}
        if isinstance(obj, date):
            return {"__type__": "date_isoformat", "value": obj.isoformat()}
        return super().default(obj)


class JobDecoder(json.JSONDecoder):
    """
    Custom JSONDecoder for Odoo records.
    """

    def __init__(self, env, *args, **kwargs):
        self.env = env
        super().__init__(*args, object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj):
        if obj.get("__type__") == "odoo_recordset":
            model_name = obj.get("model")
            ids = list(obj.get("ids") or [])
            if model_name not in self.env:
                raise ValueError(f"Model {model_name} not found in registry")

            uid = obj.get("uid")
            su = bool(obj.get("su"))
            context = obj.get("context") if isinstance(obj.get("context"), dict) else {}
            model = self.env[model_name]
            if uid is not None:
                model = model.with_user(uid)
            if context:
                model = model.with_context(context)
            if su:
                model = model.sudo()

            records = model.browse(ids)
            existing_ids = set(records.exists().ids)
            missing_ids = [
                record_id for record_id in ids if record_id not in existing_ids
            ]
            if missing_ids:
                raise ValueError(
                    f"Record(s) {missing_ids} not found in model {model_name}"
                )
            return records
        if obj.get("__type__") == "datetime_isoformat":
            return datetime.fromisoformat(obj["value"])
        if obj.get("__type__") == "date_isoformat":
            return date.fromisoformat(obj["value"])

        return obj
