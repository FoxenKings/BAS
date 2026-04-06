"""
Blueprint: Excel import from data/ folder.
Admin-only endpoint to trigger import of existing Excel files.
"""
import logging
import traceback
from flask import Blueprint, render_template, jsonify, session
from routes.common import login_required, admin_required, get_db
from services.excel_import import ExcelImportService
from services.db_cleanup import DatabaseCleanup

logger = logging.getLogger('routes.excel_import')

excel_import_bp = Blueprint('excel_import', __name__)


@excel_import_bp.route('/admin/excel-import', methods=['GET'], endpoint='excel_import_page')
@login_required
@admin_required
def excel_import_page():
    return render_template('admin/excel_import.html')


@excel_import_bp.route('/admin/excel-import/run', methods=['POST'], endpoint='excel_import_run')
@login_required
@admin_required
def excel_import_run():
    try:
        db = get_db()
        service = ExcelImportService(db, user_id=session.get('user_id', 1))
        stats = service.run()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        logger.error(f"Excel import error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        })


@excel_import_bp.route('/admin/db-cleanup', methods=['GET'], endpoint='db_cleanup_page')
@login_required
@admin_required
def db_cleanup_page():
    return render_template('admin/excel_import.html', cleanup_mode=True)


@excel_import_bp.route('/admin/db-cleanup/run', methods=['POST'], endpoint='db_cleanup_run')
@login_required
@admin_required
def db_cleanup_run():
    try:
        svc = DatabaseCleanup()
        stats = svc.run()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        })
