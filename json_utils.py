import json
import ast

def get_nested_value(data, path):
    if not path:
        return data
    current = data
    for key in path.split('.'):
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def get_parent_path(path):
    return path.rsplit('.', 1)[0] if path and '.' in path else None


def flatten_obj(d, parent_key='', sep='.'):
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten_obj(v, key, sep))
    elif isinstance(d, list):
        for i, v in enumerate(d):
            key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.update(flatten_obj(v, key, sep))
    else:
        items[parent_key] = d
    return items


def unflatten_dict(d):
    result = {}
    for key, value in d.items():
        if value in ("", None):
            continue

        if isinstance(value, str):
            val = value.strip()
            try:
                value = json.loads(val.replace("'", '"'))
            except Exception:
                try:
                    value = ast.literal_eval(val)
                except Exception:
                    pass

        cur = result
        parts = key.split('.')
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = value

    return result
