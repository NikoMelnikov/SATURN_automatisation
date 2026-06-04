from flask import Flask, render_template, request, jsonify, send_file
import os
from token_storage import save_token, delete_token, load_token
from saturn_client import get_warehouses, get_contractor_by_inn, get_invoices_on_way

app = Flask(__name__)

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), 'exports')


@app.route('/')
def index():
    saved_token = load_token()
    return render_template('index.html', saved_token=saved_token or '')


@app.route('/api/save-token', methods=['POST'])
def api_save_token():
    data = request.get_json()
    token = data.get('token', '').strip()
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен не предоставлен'})
    
    success = save_token(token)
    if success:
        return jsonify({'success': True, 'message': 'Токен сохранён'})
    return jsonify({'success': False, 'message': 'Ошибка сохранения токена'})


@app.route('/api/clear-token', methods=['POST'])
def api_clear_token():
    success = delete_token()
    if success:
        return jsonify({'success': True, 'message': 'Токен удалён'})
    return jsonify({'success': False, 'message': 'Ошибка удаления токена'})


@app.route('/api/export', methods=['POST'])
def api_export():
    data = request.get_json()
    token = data.get('token', '').strip()
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен не предоставлен'})
    
    success, message, filepath = get_warehouses(token, EXPORTS_DIR)
    
    if success:
        filename = os.path.basename(filepath)
        return jsonify({
            'success': True,
            'message': message,
            'filename': filename,
            'downloadUrl': f'/download/{filename}'
        })
    return jsonify({'success': False, 'message': message})


@app.route('/api/contractor', methods=['POST'])
def api_contractor():
    data = request.get_json()
    token = data.get('token', '').strip()
    inn = data.get('inn', '').strip()
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен не предоставлен'})
    
    if not inn:
        return jsonify({'success': False, 'message': 'Введите ИНН'})
    
    success, message, contractor_data, filepath = get_contractor_by_inn(token, inn)
    
    if success:
        filename = os.path.basename(filepath)
        return jsonify({
            'success': True,
            'message': message,
            'data': contractor_data,
            'filename': filename,
            'downloadUrl': f'/download/{filename}'
        })
    return jsonify({'success': False, 'message': message})


@app.route('/api/invoices', methods=['POST'])
def api_invoices():
    data = request.get_json()
    token = data.get('token', '').strip()
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен не предоставлен'})
    
    success, message, filepath = get_invoices_on_way(token, EXPORTS_DIR)
    
    if success:
        filename = os.path.basename(filepath)
        return jsonify({
            'success': True,
            'message': message,
            'filename': filename,
            'downloadUrl': f'/download/{filename}'
        })
    return jsonify({'success': False, 'message': message})


@app.route('/download/<filename>')
def download_file(filename):
    filepath = os.path.join(EXPORTS_DIR, filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    return jsonify({'error': 'Файл не найден'}), 404


if __name__ == '__main__':
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    app.run(host='127.0.0.1', port=5000, debug=False)
