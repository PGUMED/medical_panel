import os
import json
import ast
import csv
import io
from flask import Flask, render_template, request, redirect, url_for, flash, session, Response
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

app = Flask(__name__)
app.secret_key = 'med_key_123'

client = MongoClient('mongodb://localhost:27017/')
db = client['hospital_db']
collection = db['records']

USERS = {
    "admin": {"password": "123", "role": "admin"},
    "guest": {"password": "123", "role": "user"}
}


# --- HELPERS ---
def get_data(query=None, sort_by=None, sort_order=ASCENDING):
    if query is None: query = {}
    cursor = collection.find(query)
    if sort_by:
        cursor.sort(sort_by, sort_order)
    data = []
    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        data.append(doc)
    return data


def get_nested_value(data, path):
    if not path: return data
    keys = path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list):
            try:
                k = int(key)
                if 0 <= k < len(current):
                    current = current[k]
                else:
                    return None
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def get_parent_path(path):
    if '.' in path:
        return path.rsplit('.', 1)[0]
    return None


def flatten_obj(d, parent_key='', sep='.'):
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_obj(v, new_key, sep=sep).items())
    elif isinstance(d, list):
        for i, v in enumerate(d):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(flatten_obj(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, d))
    return dict(items)


def unflatten_dict(d):
    result = {}
    for key, value in d.items():
        if value is None or value == "": continue
        if isinstance(value, str):
            value_stripped = value.strip()
            if (value_stripped.startswith('{') and value_stripped.endswith('}')) or \
                    (value_stripped.startswith('[') and value_stripped.endswith(']')):
                try:
                    value = json.loads(value_stripped.replace("'", '"'))
                except (json.JSONDecodeError, ValueError):
                    try:
                        value = ast.literal_eval(value_stripped)
                    except (ValueError, SyntaxError):
                        pass
        parts = key.split('.')
        d_curr = result
        for part in parts[:-1]:
            if part not in d_curr: d_curr[part] = {}
            if not isinstance(d_curr[part], dict): d_curr[part] = {}
            d_curr = d_curr[part]
        d_curr[parts[-1]] = value
    return result


# --- TEMPLATE FILTERS ---
@app.template_filter('is_dict')
def is_dict(value): return isinstance(value, dict)


@app.template_filter('is_list')
def is_list(value): return isinstance(value, list)


@app.template_filter('is_list_of_dicts')
def is_list_of_dicts(value):
    return isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict)


# --- ROUTES ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = USERS.get(username)
        if user and user['password'] == password:
            session['username'] = username
            session['role'] = user['role']
            return redirect(url_for('index'))
        else:
            flash("Invalid credentials", "danger")
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
def index():
    if 'username' not in session: return redirect(url_for('login'))

    filter_key = request.args.get('filter_key')
    filter_val = request.args.get('filter_val')
    sort_by = request.args.get('sort_by')
    sort_order_str = request.args.get('sort_order', 'asc')

    mongo_query = {}
    if filter_key and filter_val:
        mongo_query[filter_key] = {"$regex": filter_val, "$options": "i"}

    mongo_sort_order = ASCENDING if sort_order_str == 'asc' else DESCENDING
    data = get_data(mongo_query, sort_by, mongo_sort_order)

    columns = []
    col_types = {}

    if data:
        all_keys = set().union(*(d.keys() for d in data))
        if '_id' in all_keys: all_keys.remove('_id')
        columns = sorted(list(all_keys))

        for col in columns:
            col_types[col] = 'string'
            for doc in data:
                val = doc.get(col)
                if isinstance(val, dict):
                    col_types[col] = 'dict'
                    break
                elif isinstance(val, list):
                    col_types[col] = 'list'
                    break
    elif filter_key:
        one_doc = collection.find_one()
        if one_doc:
            keys = set(one_doc.keys())
            if '_id' in keys: keys.remove('_id')
            columns = sorted(list(keys))

    return render_template('index.html',
                           data=data,
                           columns=columns,
                           col_types=col_types,
                           current_filter_key=filter_key,
                           current_filter_val=filter_val,
                           current_sort_by=sort_by,
                           current_sort_order=sort_order_str)


@app.route('/export')
def export_data():
    if 'username' not in session: return redirect(url_for('login'))

    doc_id = request.args.get('doc_id')
    col_path = request.args.get('col_path')
    fmt = request.args.get('format', 'json')

    filter_key = request.args.get('filter_key')
    filter_val = request.args.get('filter_val')
    sort_by = request.args.get('sort_by')
    sort_order_str = request.args.get('sort_order', 'asc')

    target_data = []

    if doc_id:
        doc = collection.find_one({"_id": ObjectId(doc_id)})
        if not doc: return "Document not found", 404
        doc['_id'] = str(doc['_id'])
        target_data = get_nested_value(doc, col_path) if col_path else doc
        filename = f"{doc_id}_{col_path.split('.')[-1] if col_path else 'full'}.{fmt}"

        if isinstance(target_data, list):
            if filter_val:
                if filter_key and len(target_data) > 0 and isinstance(target_data[0], dict):
                    target_data = [x for x in target_data if filter_val.lower() in str(x.get(filter_key, '')).lower()]
                else:
                    target_data = [x for x in target_data if filter_val.lower() in str(x).lower()]

            if sort_by:
                reverse = (sort_order_str == 'desc')
                if len(target_data) > 0 and isinstance(target_data[0], dict):
                    target_data = sorted(target_data, key=lambda x: str(x.get(sort_by, '')), reverse=reverse)

    else:
        mongo_query = {}
        if filter_key and filter_val:
            mongo_query[filter_key] = {"$regex": filter_val, "$options": "i"}
        mongo_sort_order = ASCENDING if sort_order_str == 'asc' else DESCENDING
        target_data = get_data(mongo_query, sort_by, mongo_sort_order)
        filename = f"filtered_records.{fmt}"

    if fmt == 'json':
        json_str = json.dumps(target_data, indent=2, default=str)
        return Response(json_str, mimetype="application/json",
                        headers={"Content-Disposition": f"attachment;filename={filename}"})

    elif fmt == 'csv':
        si = io.StringIO()
        if not target_data: return Response("", mimetype="text/csv")
        data_list = target_data if isinstance(target_data, list) else [target_data]

        if len(data_list) > 0 and (isinstance(data_list[0], dict) or isinstance(data_list[0], list)):
            flat_list = [flatten_obj(item) for item in data_list]
            all_keys = set().union(*(d.keys() for d in flat_list))
            fieldnames = sorted(list(all_keys))
            writer = csv.DictWriter(si, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_list)
        elif len(data_list) > 0:
            writer = csv.writer(si)
            writer.writerow(['Value'])
            for item in data_list:
                writer.writerow([item])

        output = si.getvalue()
        return Response(output, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})

    return "Invalid format", 400


# --- NEW ROUTE: ADD COLUMN GLOBALLY (Main Dashboard) ---
@app.route('/add_global_column', methods=['POST'])
def add_global_column():
    if session.get('role') != 'admin': return redirect(url_for('index'))

    new_col = request.form.get('new_col_name')
    default_val = request.form.get('default_value', '')

    if new_col:
        collection.update_many({}, {"$set": {new_col: default_val}})
        flash(f"Column '{new_col}' added to all records.", "success")

    return redirect(url_for('index'))


@app.route('/add_record', methods=['POST'])
def add_record():
    if session.get('role') != 'admin': return redirect(url_for('index'))
    try:
        new_doc = unflatten_dict(request.form.to_dict())
        collection.insert_one(new_doc)
        flash("Record created", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('index'))


@app.route('/delete_record/<string:doc_id>', methods=['POST'])
def delete_record(doc_id):
    if session.get('role') != 'admin': return redirect(url_for('index'))
    collection.delete_one({"_id": ObjectId(doc_id)})
    flash("Record deleted", "info")
    return redirect(url_for('index'))


@app.route('/details/<string:doc_id>')
@app.route('/details/<string:doc_id>/<path:col_path>')
def view_details(doc_id, col_path=None):
    if 'username' not in session: return redirect(url_for('login'))

    filter_key = request.args.get('filter_key')
    filter_val = request.args.get('filter_val')
    sort_by = request.args.get('sort_by')
    sort_order_str = request.args.get('sort_order', 'asc')

    doc = collection.find_one({"_id": ObjectId(doc_id)})
    if not doc: return redirect(url_for('index'))
    doc['_id'] = str(doc['_id'])

    target_data = get_nested_value(doc, col_path) if col_path else doc
    is_filtered = False

    if isinstance(target_data, list):
        if filter_val:
            is_filtered = True
            if filter_key and len(target_data) > 0 and isinstance(target_data[0], dict):
                target_data = [x for x in target_data if filter_val.lower() in str(x.get(filter_key, '')).lower()]
            else:
                target_data = [x for x in target_data if filter_val.lower() in str(x).lower()]

        if sort_by:
            is_filtered = True
            reverse = (sort_order_str == 'desc')
            if len(target_data) > 0 and isinstance(target_data[0], dict):
                target_data = sorted(target_data, key=lambda x: str(x.get(sort_by, '')), reverse=reverse)
            else:
                target_data = sorted(target_data, key=lambda x: str(x), reverse=reverse)

    elif isinstance(target_data, dict):
        if filter_val:
            is_filtered = True
            target_data = {k: v for k, v in target_data.items() if
                           filter_val.lower() in k.lower() or filter_val.lower() in str(v).lower()}

        reverse = (sort_order_str == 'desc')
        if sort_by == 'key':
            target_data = dict(sorted(target_data.items(), key=lambda item: item[0], reverse=reverse))
        elif sort_by == 'value':
            target_data = dict(sorted(target_data.items(), key=lambda item: str(item[1]), reverse=reverse))

    title = col_path.split('.')[-1] if col_path else "Root Record"
    parent_path = get_parent_path(col_path) if col_path else None

    return render_template('details.html',
                           data=target_data,
                           row_id=doc_id,
                           current_path=col_path if col_path else '',
                           parent_path=parent_path,
                           title=title,
                           current_filter_key=filter_key,
                           current_filter_val=filter_val,
                           current_sort_by=sort_by,
                           current_sort_order=sort_order_str,
                           is_filtered=is_filtered)


@app.route('/add_any/<string:doc_id>', methods=['POST'])
@app.route('/add_any/<string:doc_id>/<path:col_path>', methods=['POST'])
def add_any(doc_id, col_path=''):
    if session.get('role') != 'admin': return redirect(url_for('index'))

    # 1. ADD NEW COLUMN TO LIST (New Feature)
    if request.form.get('add_mode') == 'add_column':
        col_name = request.form.get('new_col_name')
        default_val = request.form.get('default_value', '')

        if col_name:
            doc = collection.find_one({"_id": ObjectId(doc_id)})
            target_list = get_nested_value(doc, col_path)

            # Loop through list and add key to every item
            if isinstance(target_list, list):
                for item in target_list:
                    if isinstance(item, dict):
                        item[col_name] = default_val

                # Save whole list back to Mongo
                collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {col_path: target_list}})
                flash(f"Column '{col_name}' added to list.", "success")

    # 2. ADD TO DICTIONARY (Standard)
    elif request.form.get('new_key_name'):
        key = request.form.get('new_key_name')
        val = request.form.get('new_key_value')
        type_ = request.form.get('val_type')
        final_val = {} if type_ == 'json_object' else val
        mongo_path = f"{col_path}.{key}" if col_path else key
        collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {mongo_path: final_val}})

    # 3. ADD TO LIST (Standard)
    elif request.form.get('new_list_item'):
        val = request.form.get('new_list_item')
        collection.update_one({"_id": ObjectId(doc_id)}, {"$push": {col_path: val}})

    # 4. ADD COMPLEX OBJECT TO LIST
    else:
        # Check if we are just adding a row (fallback)
        new_obj = unflatten_dict(request.form.to_dict())
        # Cleanup form meta fields if any
        if 'add_mode' in new_obj: del new_obj['add_mode']
        if 'new_col_name' in new_obj: del new_obj['new_col_name']

        if new_obj:
            collection.update_one({"_id": ObjectId(doc_id)}, {"$push": {col_path: new_obj}})
            flash("Data added", "success")

    return redirect(url_for('view_details', doc_id=doc_id, col_path=col_path))


@app.route('/delete_nested/<string:doc_id>/<path:col_path>/<string:key_or_index>', methods=['POST'])
def delete_nested(doc_id, col_path, key_or_index):
    if session.get('role') != 'admin': return redirect(url_for('index'))
    try:
        index = int(key_or_index)
        doc = collection.find_one({"_id": ObjectId(doc_id)})
        arr = get_nested_value(doc, col_path)
        if isinstance(arr, list) and len(arr) > index:
            arr.pop(index)
            collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {col_path: arr}})
    except ValueError:
        mongo_path = f"{col_path}.{key_or_index}"
        collection.update_one({"_id": ObjectId(doc_id)}, {"$unset": {mongo_path: ""}})
    return redirect(url_for('view_details', doc_id=doc_id, col_path=col_path))


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)