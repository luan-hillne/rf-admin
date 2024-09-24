from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from handler import Handler
import warnings
import os
import time

warnings.filterwarnings('ignore')

app = Flask(__name__)
CORS(app)

handler = Handler()


@app.route('/', methods=['GET'])
def health_check():
    return "Back-end is ready to shill!"


@app.route('/extract-rules', methods=['POST'])
def extract_rules():
    json_data = request.get_json()
    paragraph = json_data["paragraph"]
    response = handler.extract_rules(paragraph)
    return jsonify(response)


@app.route('/save-rule-extraction', methods=['POST'])
def save_rule_extraction():
    json_data = request.get_json()
    response = handler.save_rule_extraction(json_data)
    return response


@app.route('/extract-variables', methods=['POST'])
def extract_variables():
    json_data = request.get_json()
    paragraph = json_data["paragraph"]
    response = handler.extract_variables(paragraph)
    return response



@app.route('/save-variable-extraction', methods=['POST'])
def save_variable_extraction():
    json_data = request.get_json()
    response = handler.save_variable_extraction(json_data)
    return response


@app.route('/get-all-document-ids', methods=['GET'])
def get_all_document_ids():
    response = handler.get_all_document_ids()
    return jsonify(response)


@app.route('/get-all-documents', methods=['GET'])
def get_all_documents():
    response = handler.get_all_documents()
    return jsonify(response)


@app.route('/get-document', methods=['POST'])
def get_document():
    json_data = request.get_json()
    document_id = json_data["document_id"]
    response = handler.get_document(document_id)
    return jsonify(response)


@app.route('/get-paragraph', methods=['POST'])
def get_paragraph():
    json_data = request.get_json()
    paragraph_id = json_data["paragraph_id"]
    response = handler.get_paragraph(paragraph_id)
    print(response)
    return jsonify(response)


@app.route('/get-rules', methods=['POST'])
def get_rules():
    json_data = request.get_json()
    rule_ids = json_data["rule_ids"]
    response = handler.get_rules(rule_ids)
    return jsonify(response)


@app.route('/get-all-rules', methods=['GET'])
def get_all_rules():
    response = handler.get_all_rules()
    return jsonify(response)


@app.route('/get-top-variables', methods=['POST'])
def get_top_variables():
    json_data = request.get_json()
    top_k = int(json_data["top_k"])
    response = handler.get_top_variables(top_k=top_k)
    return jsonify(response)


@app.route('/get-variables-extraction', methods=['POST'])
def get_variables_extraction():
    json_data = request.get_json()
    response = handler.get_variables_extraction(json_data)
    return jsonify(response)


@app.route('/search-variable', methods=['POST'])
def search_variable():
    json_data = request.get_json()
    text_search = json_data["text_search"]
    search_type = json_data["search_type"]
    top_k = json_data["top_k"]
    response = handler.search_variable(text_search, search_type, top_k)
    return jsonify(response)


@app.route('/get-all-variables', methods=['GET'])
def get_all_variables():
    response = handler.get_all_variables()
    return jsonify(response)


@app.route('/get-variables', methods=['POST'])
def get_variables():
    json_data = request.get_json()
    list_variable_name = json_data["list_variable_name"]
    response = handler.get_variables(list_variable_name)
    return jsonify(response)


@app.route('/update-variables-on-system', methods=['POST'])
def update_variables_on_system():
    json_data = request.get_json()
    system_variables = json_data["system_variables"]
    manual_variables = json_data["manual_variables"]
    response = handler.update_variables_on_system(system_variables, manual_variables)
    return response


@app.route('/update-rule', methods=['POST'])
def update_rule():
    json_data = request.get_json()
    rule_id = json_data["rule_id"]
    rule_data = json_data["rule_data"]
    response = handler.update_rule(rule_id, rule_data)
    return response


@app.route('/unbind-variable', methods=['POST'])
def unbind_variable():
    json_data = request.get_json()
    variable_name = json_data["variable_name"]
    document_id = json_data["document_id"]
    return handler.unbind_variable(variable_name, document_id)


@app.route('/delete-rule', methods=['POST'])
def delete_rule():
    json_data = request.get_json()
    rule_id = json_data["rule_id"]
    response = handler.delete_rule(rule_id)
    return response


@app.route('/delete-document', methods=['POST'])
def delete_document():
    json_data = request.get_json()
    document_id = json_data["document_id"]
    response = handler.delete_document(document_id)
    return response


@app.route('/delete-document-replace', methods=['POST'])
def delete_document_replace():
    json_data = request.get_json()
    document_id = json_data["document_id"]
    document_replaced_id = json_data["document_replaced_id"]

    response = handler.delete_document_replace(document_id, document_replaced_id)
    return response


@app.route('/delete-document-modify', methods=['POST'])
def delete_document_modify():
    json_data = request.get_json()
    document_id = json_data["document_id"]
    document_modified_id = json_data["document_modified_id"]

    response = handler.delete_document_modify(document_id, document_modified_id)
    return response



@app.route("/gen-code", methods=["POST"])
def gen_code():
    json_data = request.get_json()
    formats = json_data['formats']
    rule_ids = json_data['rule_ids']
    for code_format in formats:
        handler.gen_code(rule_ids, code_format)
    return "200"


@app.route('/download-code-python', methods=['GET'])
def download_code_python():
    path = os.path.join("data", "tmp_code", "code.py")
    return send_file(path, as_attachment=True)


@app.route('/download-code-sql', methods=['GET'])
def download_code_sql():
    path = os.path.join("data", "tmp_code", "code.sql")
    return send_file(path, as_attachment=True)


@app.route('/download-code-java', methods=['GET'])
def download_code_java():
    path = os.path.join("data", "tmp_code", "code.java")
    return send_file(path, as_attachment=True)


@app.route('/check-conflict', methods=['POST'])
def check_conflict():
    json_data = request.get_json()
    active_document_ids = json_data["active_document_ids"]
    draft_document_ids = json_data["draft_document_ids"]
    list_output_name = json_data["list_output_name"]
    response = handler.check_conflict(active_document_ids, draft_document_ids, list_output_name)
    return jsonify(response)


@app.route('/check-gap', methods=['POST'])
def check_gap():
    json_data = request.get_json()
    active_document_ids = json_data["active_document_ids"]
    draft_document_ids = json_data["draft_document_ids"]
    list_output_name = json_data["list_output_name"]
    response = handler.check_gap(active_document_ids, draft_document_ids, list_output_name)
    return jsonify(response)


@app.route('/get-all-output-name', methods=['GET'])
def get_all_output_name():
    response = handler.get_all_output_name()
    return jsonify(response)


@app.route('/init-variable', methods=['POST'])
def init_variable():
    data = request.get_json()
    response = handler.init_variable(data)
    return response

@app.route('/update-variable', methods=['POST'])
def update_variable():
    data = request.get_json()
    response = handler.update_variable(data)
    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9999, debug=True)