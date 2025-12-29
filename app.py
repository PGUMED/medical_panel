import os
import json
import ast
import csv
import io
from flask import Flask, render_template, request, redirect, url_for, flash, session, Response
from pymongo import MongoClient
from bson.objectid import ObjectId

app = Flask(__name__)
app.secret_key = 'med_key_123'  # Change this in production

# --- MONGODB CONNECTION ---
# If using Docker: 'mongodb://admin:password123@localhost:27017/'
# If using Local Desktop: 'mongodb://localhost:27017/'
client = MongoClient('mongodb://localhost:27017/')
db = client['hospital_db']
collection = db['records']

# --- USERS ---
USERS = {
    "admin": {"password": "123", "role": "admin"},
    "guest": {"password": "123", "role": "user"}
}


# --- HELPER FUNCTIONS ---

def get_data():
    """Fetches all documents, converting ObjectId to string."""
    cursor = collection.find()
    data = []
    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        data.append(doc)
    return data


def get_nested_value(data, path):
    """Traverses dictionary using dot notation."""
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
    """
    Flattens a nested dictionary AND lists recursively.
    - Dict keys become: parent.child
    - List items become: parent.0.child, parent.1.child
    """
    items = []

    # CASE 1: DICTIONARY
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_obj(v, new_key, sep=sep).items())

    # CASE 2: LIST
    elif isinstance(d, list):
        for i, v in enumerate(d):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(flatten_obj(v, new_key, sep=sep).items())

    # CASE 3: PRIMITIVE VALUE
    else:
        items.append((parent_key, d))

    return dict(items)


def unflatten_dict(d):
    """
    Converts form data into nested objects.
    Handles JSON strings like '{}' or '[]' explicitly to create real Objects/Lists.
    """
    result = {}
    for key, value in d.items():
        if value is None or value == "":
            continue

        # Try to parse JSON/Python literals (e.g., "{}", "[]", "{'a':1}")
        if isinstance(value, str):
            value_stripped = value.strip()
            # If it looks like a Dict or List, try to parse it
            if (value_stripped.startswith('{') and value_stripped.endswith('}')) or \
                    (value_stripped.startswith('[') and value_stripped.endswith(']')):
                try:
                    # Prefer standard JSON
                    value = json.loads(value_stripped.replace("'", '"'))
                except (json.JSONDecodeError, ValueError):
                    try:
                        # Fallback to Python syntax
                        value = ast.literal_eval(value_stripped)
                    except (ValueError, SyntaxError):
                        pass  # Keep as string if parsing fails

        # Standard dot-notation unflattening
        parts = key.split('.')
        d_curr = result
        for part in parts[:-1]:
            if part not in d_curr:
                d_curr[part] = {}

            # Safety Check: If d_curr[part] exists but is a STRING (from a previous overwrite),
            # force convert it to a dict to allow nesting.
            if not isinstance(d_curr[part], dict):
                d_curr[part] = {}

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

    data = get_data()

    columns = []
    col_types = {}  # Store inferred types for the frontend badges

    if data:
        # Collect all unique top-level keys
        all_keys = set().union(*(d.keys() for d in data))
        if '_id' in all_keys: all_keys.remove('_id')
        columns = sorted(list(all_keys))

        # Detect types (Dict, List, or String) based on existing data
        for col in columns:
            col_types[col] = 'string'  # Default
            for doc in data:
                val = doc.get(col)
                if isinstance(val, dict):
                    col_types[col] = 'dict'
                    break
                elif isinstance(val, list):
                    col_types[col] = 'list'
                    break

    return render_template('index.html', data=data, columns=columns, col_types=col_types)


@app.route('/export')
def export_data():
    if 'username' not in session: return redirect(url_for('login'))

    doc_id = request.args.get('doc_id')
    col_path = request.args.get('col_path')
    fmt = request.args.get('format', 'json')

    # 1. Fetch Data
    if doc_id:
        doc = collection.find_one({"_id": ObjectId(doc_id)})
        if not doc: return "Document not found", 404
        doc['_id'] = str(doc['_id'])
        target_data = get_nested_value(doc, col_path) if col_path else doc
        filename = f"{doc_id}_{col_path.split('.')[-1] if col_path else 'full'}.{fmt}"
    else:
        target_data = get_data()
        filename = f"all_records.{fmt}"

    # 2. JSON Export
    if fmt == 'json':
        json_str = json.dumps(target_data, indent=2, default=str)
        return Response(
            json_str,
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )

    # 3. CSV Export (Horizontal Layout)
    elif fmt == 'csv':
        si = io.StringIO()

        if not target_data:
            return Response("", mimetype="text/csv")

        # Normalize: Ensure we always work with a LIST
        # If it's a single dict (Single Record View), wrap it in a list [dict]
        data_list = target_data if isinstance(target_data, list) else [target_data]

        if len(data_list) > 0 and (isinstance(data_list[0], dict) or isinstance(data_list[0], list)):
            # Flatten objects recursively
            flat_list = [flatten_obj(item) for item in data_list]

            # Dynamic Header Extraction
            all_keys = set().union(*(d.keys() for d in flat_list))
            fieldnames = sorted(list(all_keys))

            writer = csv.DictWriter(si, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_list)

        elif len(data_list) > 0:
            # Primitives
            writer = csv.writer(si)
            writer.writerow(['Value'])
            for item in data_list:
                writer.writerow([item])

        output = si.getvalue()
        return Response(
            output,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )

    return "Invalid format", 400


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

    doc = collection.find_one({"_id": ObjectId(doc_id)})
    if not doc: return redirect(url_for('index'))
    doc['_id'] = str(doc['_id'])

    target_data = get_nested_value(doc, col_path) if col_path else doc

    # If it is a list of dicts, we want to ensure it displays nicely in the table
    # The template handles list_of_dicts rendering logic.

    title = col_path.split('.')[-1] if col_path else "Root Record"
    parent_path = get_parent_path(col_path) if col_path else None

    return render_template('details.html',
                           data=target_data,
                           row_id=doc_id,
                           current_path=col_path if col_path else '',
                           parent_path=parent_path,
                           title=title)


@app.route('/add_any/<string:doc_id>', methods=['POST'])
@app.route('/add_any/<string:doc_id>/<path:col_path>', methods=['POST'])
def add_any(doc_id, col_path=''):
    if session.get('role') != 'admin': return redirect(url_for('index'))

    # Update Logic
    if request.form.get('new_key_name'):
        key = request.form.get('new_key_name')
        val = request.form.get('new_key_value')
        type_ = request.form.get('val_type')
        final_val = {} if type_ == 'json_object' else val

        mongo_path = f"{col_path}.{key}" if col_path else key
        collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {mongo_path: final_val}})

    elif request.form.get('new_list_item'):
        val = request.form.get('new_list_item')
        collection.update_one({"_id": ObjectId(doc_id)}, {"$push": {col_path: val}})

    else:
        new_obj = unflatten_dict(request.form.to_dict())
        collection.update_one({"_id": ObjectId(doc_id)}, {"$push": {col_path: new_obj}})

    flash("Data added", "success")
    return redirect(url_for('view_details', doc_id=doc_id, col_path=col_path))


@app.route('/delete_nested/<string:doc_id>/<path:col_path>/<string:key_or_index>', methods=['POST'])
def delete_nested(doc_id, col_path, key_or_index):
    if session.get('role') != 'admin': return redirect(url_for('index'))

    try:
        index = int(key_or_index)
        # Deleting from list: Read, Pop, Save
        doc = collection.find_one({"_id": ObjectId(doc_id)})
        arr = get_nested_value(doc, col_path)
        if isinstance(arr, list) and len(arr) > index:
            arr.pop(index)
            collection.update_one({"_id": ObjectId(doc_id)}, {"$set": {col_path: arr}})
    except ValueError:
        # Deleting Dict Key
        mongo_path = f"{col_path}.{key_or_index}"
        collection.update_one({"_id": ObjectId(doc_id)}, {"$unset": {mongo_path: ""}})

    return redirect(url_for('view_details', doc_id=doc_id, col_path=col_path))


if __name__ == '__main__':
    app.run(debug=True)