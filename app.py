import os
import json
from flask import Flask, render_template, request, redirect, url_for, flash

app = Flask(__name__)
app.secret_key = 'med_key_123'
DATA_FILE = 'data.json'


# --- 1. DATA HELPERS ---
def get_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except:
                return []
    return []


def save_data(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def get_nested_value(data, path):
    keys = path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


# --- NEW HELPER: CALCULATE PARENT PATH ---
def get_parent_path(path):
    if '.' in path:
        # "Medical_Record.Diagnostics" -> "Medical_Record"
        return path.rsplit('.', 1)[0]
    return None  # We are at the top level


def flatten_obj(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_obj(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def unflatten_dict(d):
    result = {}
    for key, value in d.items():
        if not value: continue
        if key.split('.')[-1].lower() == 'id' and value.isdigit():
            value = int(value)
        parts = key.split('.')
        d_curr = result
        for part in parts[:-1]:
            if part not in d_curr: d_curr[part] = {}
            d_curr = d_curr[part]
        d_curr[parts[-1]] = value
    return result


# --- FILTERS ---
@app.template_filter('is_dict')
def is_dict(value): return isinstance(value, dict)


@app.template_filter('is_list')
def is_list(value): return isinstance(value, list)


@app.template_filter('is_list_of_dicts')
def is_list_of_dicts(value):
    return isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict)


# --- ROUTES ---

@app.route('/')
def index():
    data = get_data()
    columns = []
    if len(data) > 0:
        columns = list(data[0].keys())
    return render_template('index.html', data=data, columns=columns)


@app.route('/details/<int:row_id>/<path:col_path>')
def view_details(row_id, col_path):
    full_data = get_data()
    if row_id < 0 or row_id >= len(full_data): return redirect(url_for('index'))

    target_data = get_nested_value(full_data[row_id], col_path)
    if target_data is None: return "Path not found", 404

    display_data = target_data
    if isinstance(target_data, list) and len(target_data) > 0 and isinstance(target_data[0], dict):
        display_data = [flatten_obj(row) for row in target_data]

    title = col_path.split('.')[-1].replace('_', ' ')

    # Calculate Parent Path for the "Back" button
    parent_path = get_parent_path(col_path)

    return render_template('details.html',
                           data=display_data,
                           title=title,
                           row_id=row_id,
                           current_path=col_path,
                           parent_path=parent_path)  # Pass to template


@app.route('/add_any/<int:row_id>/<path:col_path>', methods=['POST'])
def add_any(row_id, col_path):
    full_data = get_data()
    target_node = get_nested_value(full_data[row_id], col_path)

    if isinstance(target_node, list):
        raw_form = request.form.to_dict()
        new_item = unflatten_dict(raw_form)
        target_node.append(new_item)
        flash("New row added.", "success")
    elif isinstance(target_node, dict):
        new_key = request.form.get('new_key_name')
        new_val = request.form.get('new_key_value')
        val_type = request.form.get('val_type')
        if new_key:
            if val_type == 'json_object':
                target_node[new_key] = {}
            else:
                target_node[new_key] = new_val
            flash(f"Key '{new_key}' added.", "success")

    save_data(full_data)
    return redirect(url_for('view_details', row_id=row_id, col_path=col_path))


if __name__ == '__main__':
    app.run(debug=True)