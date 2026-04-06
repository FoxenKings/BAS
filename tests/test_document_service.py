"""
Юнит-тесты для DocumentService.
Используем unittest.mock — БД не нужна.
"""
import pytest
from unittest.mock import MagicMock, call
from services.document_service import DocumentService
from exceptions import NotFoundError, BusinessRuleError, ValidationError


def _make_doc(**kwargs):
    """Фабрика тестового документа."""
    defaults = {
        'id': 1,
        'doc_type': 'receipt',
        'status': 'draft',
        'posted_by': None,
        'posted_at': None,
    }
    defaults.update(kwargs)
    return defaults


@pytest.fixture
def db():
    return MagicMock()


@pytest.fixture
def svc(db):
    return DocumentService(db)


# ─── post_document ────────────────────────────────────────────────────────────

class TestPostDocument:
    def test_posts_draft_document(self, svc, db):
        db.execute_query.side_effect = [
            _make_doc(id=1, doc_type='receipt', status='draft'),  # SELECT
            None,  # UPDATE
        ]
        result = svc.post_document(1, user_id=10)
        assert result['success'] is True
        assert '1' in result['message']

        # Проверяем что UPDATE был вызван с правильными аргументами
        update_call = db.execute_query.call_args_list[1]
        sql, params = update_call[0]
        assert 'posted' in sql
        assert params == (10, 1)

    def test_raises_not_found_for_missing_document(self, svc, db):
        db.execute_query.return_value = None
        with pytest.raises(NotFoundError) as exc_info:
            svc.post_document(999, user_id=1)
        assert exc_info.value.context['entity_id'] == 999

    def test_raises_business_rule_if_already_posted(self, svc, db):
        db.execute_query.return_value = _make_doc(status='posted')
        with pytest.raises(BusinessRuleError) as exc_info:
            svc.post_document(1, user_id=1)
        assert exc_info.value.context['rule'] == 'document_already_posted'

    def test_raises_validation_for_unknown_doc_type(self, svc, db):
        db.execute_query.return_value = _make_doc(doc_type='unknown_type', status='draft')
        with pytest.raises(ValidationError) as exc_info:
            svc.post_document(1, user_id=1)
        assert exc_info.value.context['field'] == 'doc_type'

    @pytest.mark.parametrize('doc_type', ['receipt', 'issuance', 'transfer', 'write-off', 'return'])
    def test_posts_all_valid_document_types(self, svc, db, doc_type):
        db.execute_query.side_effect = [
            _make_doc(doc_type=doc_type, status='draft'),
            None,
        ]
        result = svc.post_document(1, user_id=1)
        assert result['success'] is True


# ─── generate_document_number ─────────────────────────────────────────────────

class TestGenerateDocumentNumber:
    @pytest.mark.parametrize('doc_type,expected_prefix', [
        ('receipt', 'RC'),
        ('issuance', 'IS'),
        ('transfer', 'TR'),
        ('write-off', 'WO'),
        ('return', 'RN'),
    ])
    def test_correct_prefix(self, svc, db, doc_type, expected_prefix):
        db.execute_query.return_value = {'cnt': 0}
        number = svc.generate_document_number(doc_type)
        assert number.startswith(expected_prefix + '-')

    def test_sequence_increments(self, svc, db):
        db.execute_query.return_value = {'cnt': 41}
        number = svc.generate_document_number('issuance')
        assert number == 'IS-0042'

    def test_first_document_is_0001(self, svc, db):
        db.execute_query.return_value = {'cnt': 0}
        number = svc.generate_document_number('receipt')
        assert number == 'RC-0001'

    def test_unknown_type_uses_doc_prefix(self, svc, db):
        db.execute_query.return_value = {'cnt': 0}
        number = svc.generate_document_number('custom_type')
        assert number.startswith('DOC-')

    def test_zero_padding_to_4_digits(self, svc, db):
        db.execute_query.return_value = {'cnt': 999}
        number = svc.generate_document_number('receipt')
        assert number == 'RC-1000'

    def test_handles_none_db_response(self, svc, db):
        db.execute_query.return_value = None
        number = svc.generate_document_number('receipt')
        assert number == 'RC-0001'


# ─── validate_document ────────────────────────────────────────────────────────

class TestValidateDocument:
    def test_valid_data_passes(self, svc):
        svc.validate_document({'doc_type': 'receipt', 'warehouse_id': 1})

    def test_raises_if_doc_type_missing(self, svc):
        with pytest.raises(ValidationError) as exc_info:
            svc.validate_document({'warehouse_id': 1})
        assert exc_info.value.context['field'] == 'doc_type'

    def test_raises_if_doc_type_invalid(self, svc):
        with pytest.raises(ValidationError) as exc_info:
            svc.validate_document({'doc_type': 'garbage', 'warehouse_id': 1})
        assert exc_info.value.context['field'] == 'doc_type'
        assert exc_info.value.context['value'] == 'garbage'

    def test_raises_if_warehouse_missing(self, svc):
        with pytest.raises(ValidationError) as exc_info:
            svc.validate_document({'doc_type': 'receipt'})
        assert exc_info.value.context['field'] == 'warehouse_id'

    @pytest.mark.parametrize('doc_type', ['receipt', 'issuance', 'transfer', 'write-off', 'return'])
    def test_all_valid_types_pass(self, svc, doc_type):
        svc.validate_document({'doc_type': doc_type, 'warehouse_id': 1})
