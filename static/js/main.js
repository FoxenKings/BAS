// ======================================== //
// ГЛАВНЫЙ ФАЙЛ JAVASCRIPT
// ======================================== //

// Глобальные настройки
const CONFIG = {
    dateFormat: 'DD.MM.YYYY',
    dateTimeFormat: 'DD.MM.YYYY HH:mm',
    currency: 'RUB',
    debounceDelay: 500,
    alertTimeout: 5000,
    apiTimeout: 30000
};

// ======================================== //
// ИНИЦИАЛИЗАЦИЯ ПРИ ЗАГРУЗКЕ
// ======================================== //

$(document).ready(function() {
    initTooltips();
    initPopovers();
    initDateFormats();
    initSearchInputs();
    initConfirmActions();
    initSelect2();
    initDataTables();
    initMenuState();
    initAutoHideAlerts();
    initQuickSearch();
    initStatusColors();
    loadNotificationCount();
    
    // Показываем уведомление о миграции при первом посещении
    if (!localStorage.getItem('migration_notification_shown')) {
        showMigrationNotification();
        localStorage.setItem('migration_notification_shown', 'true');
    }
});

// ======================================== //
// ОСНОВНЫЕ ФУНКЦИИ ИНИЦИАЛИЗАЦИИ
// ======================================== //

function initTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initPopovers() {
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function(popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
}

function initDateFormats() {
    // Форматирование дат
    $('.format-date').each(function() {
        const dateStr = $(this).text().trim();
        if (dateStr) {
            $(this).text(formatDate(dateStr));
        }
    });
    
    $('.format-datetime').each(function() {
        const dateStr = $(this).text().trim();
        if (dateStr) {
            $(this).text(formatDateTime(dateStr));
        }
    });
}

function initSearchInputs() {
    let searchTimeout;
    $('.search-input').on('input', function() {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            $(this).closest('form').submit();
        }, CONFIG.debounceDelay);
    });
}

function initConfirmActions() {
    $('.confirm-action').click(function(e) {
        if (!confirm($(this).data('confirm') || 'Вы уверены?')) {
            e.preventDefault();
            return false;
        }
    });
}

function initSelect2() {
    if ($.fn.select2) {
        $('.select2').select2({
            theme: 'bootstrap-5',
            width: '100%',
            language: 'ru'
        });
    }
}

function initDataTables() {
    if ($.fn.DataTable) {
        $.extend(true, $.fn.dataTable.defaults, {
            language: {
                url: '//cdn.datatables.net/plug-ins/1.13.4/i18n/ru.json'
            },
            pageLength: 25,
            lengthMenu: [10, 25, 50, 100],
            stateSave: true,
            preDrawCallback: function(settings) {
                var api = this.api();
                var table = $(api.table().node());
                if (!table.data('skeleton-shown') && api.page.info().recordsTotal === 0) {
                    table.data('skeleton-shown', true);
                }
            },
            initComplete: function() {
                var table = $(this.api().table().node());
                table.removeData('skeleton-shown');
                table.closest('.datatable-wrapper, .card-body, .table-responsive')
                     .find('.dt-skeleton').remove();
            }
        });

        // Вставляем skeleton перед каждой таблицей с data-dt-skeleton
        $('table[data-dt-skeleton]').each(function() {
            var cols = parseInt($(this).data('dt-skeleton')) || 5;
            var rows = 5;
            var skeletonHtml = '<div class="dt-skeleton mb-2">';
            for (var r = 0; r < rows; r++) {
                skeletonHtml += '<div class="d-flex gap-2 mb-2">';
                for (var c = 0; c < cols; c++) {
                    skeletonHtml += '<div class="skeleton skeleton-cell flex-fill" style="height:20px"></div>';
                }
                skeletonHtml += '</div>';
            }
            skeletonHtml += '</div>';
            $(this).before(skeletonHtml).hide();
            var self = this;
            $(self).one('init.dt', function() {
                $(self).closest('.table-responsive, .card-body').find('.dt-skeleton').remove();
                $(self).show();
            });
        });
    }
}

function initMenuState() {
    // Сохранение состояния меню
    $('.nav-group-title[data-bs-toggle="collapse"]').each(function() {
        const target = $(this).attr('href');
        const savedState = localStorage.getItem('menu_' + target);
        
        if (savedState !== null) {
            if (savedState === 'true') {
                $(target).collapse('show');
                $(this).find('i.fa-chevron-down').removeClass('collapsed');
            } else {
                $(target).collapse('hide');
                $(this).find('i.fa-chevron-down').addClass('collapsed');
            }
        }
    });
    
    $('.nav-group-title[data-bs-toggle="collapse"]').click(function() {
        const target = $(this).attr('href');
        const isExpanded = $(target).hasClass('show');
        localStorage.setItem('menu_' + target, isExpanded);
        
        const icon = $(this).find('i.fa-chevron-down');
        if (icon.length) {
            icon.toggleClass('collapsed');
        }
    });
    
    // Подсветка активного пункта меню
    $('.sidebar .nav-link').each(function() {
        if (window.location.href.includes($(this).attr('href'))) {
            $(this).addClass('active');
            
            // Раскрываем родительское меню
            const parentCollapse = $(this).closest('.nav-group-content');
            if (parentCollapse.length) {
                parentCollapse.collapse('show');
                const collapseId = parentCollapse.attr('id');
                $(`[href="#${collapseId}"]`).find('i.fa-chevron-down').removeClass('collapsed');
                localStorage.setItem('menu_#' + collapseId, true);
            }
        }
    });
}

function initAutoHideAlerts() {
    // Авто-скрытие только помеченных алертов (data-auto-dismiss="ms")
    $('[data-auto-dismiss]').each(function() {
        var $el = $(this);
        var delay = parseInt($el.data('auto-dismiss')) || 4000;
        setTimeout(function() {
            $el.fadeOut(400, function() { $el.alert('close'); });
        }, delay);
    });
}

function loadNotificationCount() {
    var $count = $('#topbarNotifCount');
    var $sidebar = $('.notification-badge');
    $.getJSON('/api/notifications/counts', function(data) {
        var n = data.unread || 0;
        if (n > 0) {
            var label = n > 99 ? '99+' : String(n);
            $count.text(label).show();
            $sidebar.text(label).show();
        } else {
            $count.hide();
            $sidebar.hide();
        }
    }).fail(function() {
        $count.hide();
    });
}

function initQuickSearch() {
    // Быстрый поиск на дашборде
    $('#quickSearch').on('keypress', function(e) {
        if (e.key === 'Enter') {
            const query = this.value.trim();
            if (query) {
                // Используем поиск по экземплярам
                window.location.href = "/instances?search=" + encodeURIComponent(query);
            }
        }
    });
}

function initStatusColors() {
    // Добавляем классы статусов
    $('[data-status]').each(function() {
        const status = $(this).data('status');
        $(this).addClass('status-' + status);
    });
}

// ======================================== //
// API ФУНКЦИИ
// ======================================== //

const API = {
    // Загрузка данных с обработкой ошибок
    get: function(url, params = {}) {
        return $.ajax({
            url: url,
            method: 'GET',
            data: params,
            timeout: CONFIG.apiTimeout,
            error: this.handleError
        });
    },
    
    post: function(url, data) {
        return $.ajax({
            url: url,
            method: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(data),
            timeout: CONFIG.apiTimeout,
            error: this.handleError
        });
    },
    
    put: function(url, data) {
        return $.ajax({
            url: url,
            method: 'PUT',
            contentType: 'application/json',
            data: JSON.stringify(data),
            timeout: CONFIG.apiTimeout,
            error: this.handleError
        });
    },
    
    delete: function(url) {
        return $.ajax({
            url: url,
            method: 'DELETE',
            timeout: CONFIG.apiTimeout,
            error: this.handleError
        });
    },
    
    handleError: function(xhr) {
        let message = 'Ошибка запроса';
        
        if (xhr.responseJSON && xhr.responseJSON.error) {
            message = xhr.responseJSON.error;
        } else if (xhr.status === 401) {
            message = 'Необходима авторизация';
            window.location.href = '/login';
        } else if (xhr.status === 403) {
            message = 'Недостаточно прав';
        } else if (xhr.status === 404) {
            message = 'Ресурс не найден';
        } else if (xhr.status === 500) {
            message = 'Внутренняя ошибка сервера';
        }
        
        showAlert(message, 'error');
        return Promise.reject(xhr);
    }
};

// ======================================== //
// УВЕДОМЛЕНИЯ
// ======================================== //

function showAlert(message, type = 'info') {
    const alertClass = {
        'success': 'alert-success',
        'error': 'alert-danger',
        'warning': 'alert-warning',
        'info': 'alert-info'
    }[type] || 'alert-info';
    
    const icon = {
        'success': 'fa-check-circle',
        'error': 'fa-exclamation-circle',
        'warning': 'fa-exclamation-triangle',
        'info': 'fa-info-circle'
    }[type] || 'fa-info-circle';
    
    const alertHtml = `
        <div class="alert ${alertClass} alert-dismissible fade show" role="alert">
            <div class="d-flex align-items-center">
                <i class="fas ${icon} me-2"></i>
                <div>${message}</div>
                <button type="button" class="btn-close ms-auto" data-bs-dismiss="alert"></button>
            </div>
        </div>
    `;
    
    // Добавляем в начало контейнера
    $('.container-fluid.mt-2').prepend(alertHtml);
    
    // Автоматическое скрытие
    setTimeout(() => {
        $('.alert').alert('close');
    }, CONFIG.alertTimeout);
}

function showToast(message, type = 'info') {
    if (typeof toastr !== 'undefined') {
        toastr[type](message);
    } else {
        showAlert(message, type);
    }
}

function showLoading(message = 'Загрузка...') {
    if ($('.spinner-overlay').length === 0) {
        $('body').append(`
            <div class="spinner-overlay">
                <div class="bg-white p-4 rounded-3 shadow-lg text-center">
                    <div class="spinner-border text-primary mb-3" style="width: 3rem; height: 3rem;" role="status">
                        <span class="visually-hidden">Загрузка...</span>
                    </div>
                    <div class="text-dark fw-bold">${message}</div>
                </div>
            </div>
        `);
    }
}

function hideLoading() {
    $('.spinner-overlay').fadeOut(300, function() {
        $(this).remove();
    });
}

// ======================================== //
// ФОРМАТИРОВАНИЕ
// ======================================== //

function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined) return '0';
    return new Intl.NumberFormat('ru-RU', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(num);
}

function formatCurrency(amount) {
    if (amount === null || amount === undefined) return '0,00 ₽';
    return new Intl.NumberFormat('ru-RU', {
        style: 'currency',
        currency: CONFIG.currency,
        minimumFractionDigits: 2
    }).format(amount);
}

function formatDate(dateStr) {
    if (!dateStr) return '—';
    if (typeof moment !== 'undefined') {
        return moment(dateStr).format('DD.MM.YYYY');
    }
    try {
        const date = new Date(dateStr);
        return date.toLocaleDateString('ru-RU');
    } catch (e) {
        return dateStr;
    }
}

function formatDateTime(dateStr) {
    if (!dateStr) return '—';
    if (typeof moment !== 'undefined') {
        return moment(dateStr).format('DD.MM.YYYY HH:mm');
    }
    try {
        const date = new Date(dateStr);
        return date.toLocaleString('ru-RU');
    } catch (e) {
        return dateStr;
    }
}

// ======================================== //
// ВАЛИДАЦИЯ
// ======================================== //

function validateForm(formId) {
    const form = document.getElementById(formId);
    if (!form.checkValidity()) {
        form.classList.add('was-validated');
        return false;
    }
    return true;
}

function validateEmail(email) {
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return re.test(email);
}

function validatePhone(phone) {
    const re = /^[\d\s\-+()]+$/;
    return re.test(phone);
}

// ======================================== //
// РАБОТА С ФАЙЛАМИ
// ======================================== //

function uploadFile(url, file, onProgress, onSuccess, onError) {
    const formData = new FormData();
    formData.append('file', file);

    // fetch не поддерживает onProgress — для прогресса оставляем XHR только там где нужен прогресс
    if (onProgress) {
        const xhr = new XMLHttpRequest();
        xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
                onProgress(Math.round((e.loaded / e.total) * 100));
            }
        });
        xhr.onload = function() {
            if (xhr.status === 200) {
                let response;
                try { response = JSON.parse(xhr.responseText); } catch (e) { response = xhr.responseText; }
                if (onSuccess) onSuccess(response);
                showToast('Файл успешно загружен', 'success');
            } else {
                let error;
                try { error = JSON.parse(xhr.responseText).error; } catch (e) { error = xhr.responseText || 'Ошибка загрузки файла'; }
                if (onError) onError(error);
                showToast(error, 'error');
            }
        };
        xhr.onerror = function() {
            const error = 'Network error';
            if (onError) onError(error);
            showToast(error, 'error');
        };
        xhr.open('POST', url, true);
        xhr.send(formData);
        return;
    }

    fetch(url, { method: 'POST', body: formData })
        .then(function(res) {
            return res.text().then(function(text) {
                let data;
                try { data = JSON.parse(text); } catch (e) { data = text; }
                if (res.ok) {
                    if (onSuccess) onSuccess(data);
                    showToast('Файл успешно загружен', 'success');
                } else {
                    const error = (data && data.error) ? data.error : (text || 'Ошибка загрузки файла');
                    if (onError) onError(error);
                    showToast(error, 'error');
                }
            });
        })
        .catch(function(err) {
            const error = err.message || 'Network error';
            if (onError) onError(error);
            showToast(error, 'error');
        });
}

function exportData(format, data, filename) {
    let mimeType, content;
    
    switch (format) {
        case 'csv':
            mimeType = 'text/csv';
            content = arrayToCSV(data);
            break;
        case 'json':
            mimeType = 'application/json';
            content = JSON.stringify(data, null, 2);
            break;
        case 'excel':
            window.location.href = `/export/${filename}`;
            return;
        default:
            showToast('Неподдерживаемый формат', 'error');
            return;
    }
    
    const blob = new Blob([content], { type: mimeType });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${filename}.${format}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
    
    showToast(`Файл ${filename}.${format} сохранен`, 'success');
}

function arrayToCSV(data) {
    if (!data || data.length === 0) return '';
    
    const headers = Object.keys(data[0]);
    const rows = data.map(row => 
        headers.map(header => {
            const cell = row[header];
            if (cell === null || cell === undefined) return '';
            if (typeof cell === 'string') {
                return `"${cell.replace(/"/g, '""')}"`;
            }
            if (cell instanceof Date) {
                return formatDate(cell);
            }
            return cell;
        }).join(',')
    );
    
    return [headers.join(','), ...rows].join('\n');
}

// ======================================== //
// УТИЛИТЫ
// ======================================== //

function debounce(func, wait = CONFIG.debounceDelay) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function copyToClipboard(text) {
    if (navigator.clipboard) {
        navigator.clipboard.writeText(text).then(function() {
            showToast('Скопировано в буфер обмена', 'success');
        }, function() {
            showToast('Ошибка копирования', 'error');
        });
    } else {
        // Fallback для старых браузеров
        const textarea = document.createElement('textarea');
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            showToast('Скопировано в буфер обмена', 'success');
        } catch (err) {
            showToast('Ошибка копирования', 'error');
        }
        document.body.removeChild(textarea);
    }
}

function getStatusColor(status) {
    const colors = {
        'in_stock': 'success',
        'available': 'success',
        'in_use': 'primary',
        'under_repair': 'warning',
        'repair': 'warning',
        'written_off': 'secondary',
        'expired': 'danger',
        'quarantine': 'warning',
        'approved': 'success',
        'rejected': 'danger',
        'draft': 'secondary',
        'posted': 'success',
        'cancelled': 'danger'
    };
    return colors[status] || 'primary';
}

function getStatusIcon(status) {
    const icons = {
        'in_stock': 'fa-check-circle',
        'available': 'fa-check-circle',
        'in_use': 'fa-user-check',
        'under_repair': 'fa-tools',
        'repair': 'fa-tools',
        'written_off': 'fa-trash-alt',
        'expired': 'fa-calendar-times',
        'quarantine': 'fa-exclamation-triangle',
        'approved': 'fa-check-circle',
        'rejected': 'fa-times-circle',
        'draft': 'fa-pencil-alt',
        'posted': 'fa-check-circle',
        'cancelled': 'fa-ban'
    };
    return icons[status] || 'fa-circle';
}

// ======================================== //
// ФУНКЦИИ ДЛЯ РАБОТЫ С УСТАРЕВШИМИ РАЗДЕЛАМИ
// ======================================== //

function confirmLegacy(name, newUrl = '/nomenclatures') {
    // Определяем URL в зависимости от типа
    const typeMap = {
        'активы': 'asset',
        'инструменты': 'tool',
        'оборудование': 'equipment',
        'расходники': 'consumable',
        'сиз': 'ppe'
    };
    
    const itemType = typeMap[name.toLowerCase()];
    const targetUrl = itemType ? `${newUrl}?item_type=${itemType}` : newUrl;
    
    if (confirm(`Раздел "${name}" перенесен в новую архитектуру.\n\nПерейти к обновленной номенклатуре?`)) {
        window.location.href = targetUrl;
    }
    return false;
}

function showMigrationNotification() {
    const notification = `
        <div class="alert alert-info alert-dismissible fade show migration-alert" role="alert" 
             style="position: fixed; top: 20px; right: 20px; z-index: 9999; max-width: 400px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
            <div class="d-flex align-items-center">
                <i class="fas fa-info-circle fa-2x me-3 text-info"></i>
                <div>
                    <strong>Новая архитектура!</strong>
                    <p class="mb-0 small">Система переведена на номенклатурный учет. Используйте новые разделы меню.</p>
                </div>
            </div>
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    $('body').append(notification);
    
    setTimeout(function() {
        $('.migration-alert').fadeOut(500, function() {
            $(this).remove();
        });
    }, 10000);
}

// ======================================== //
// ФУНКЦИИ ДЛЯ КАТЕГОРИЙ
// ======================================== //

function loadCategoryRules(categoryId) {
    showLoading('Загрузка правил...');
    
    API.get(`/api/categories/${categoryId}/rules`)
        .then(function(rules) {
            hideLoading();
            displayCategoryRules(rules);
        })
        .catch(function() {
            hideLoading();
        });
}

function displayCategoryRules(rules) {
    const container = $('#categoryRulesContainer');
    if (!container.length) return;
    
    if (!rules || rules.length === 0) {
        container.html('<p class="text-muted">Нет правил для этой категории</p>');
        return;
    }
    
    let html = '<div class="list-group">';
    rules.forEach(rule => {
        html += `
            <div class="list-group-item">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <span class="badge bg-${getRuleTypeColor(rule.rule_type)}">${rule.rule_type}</span>
                        <span class="ms-2">${rule.keyword || rule.pattern || ''}</span>
                    </div>
                    <div>
                        <span class="badge bg-${getPriorityColor(rule.priority)}">${rule.priority}</span>
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';
    
    container.html(html);
}

function getRuleTypeColor(type) {
    const colors = {
        'name': 'primary',
        'model': 'info',
        'serial_prefix': 'warning',
        'attribute': 'success'
    };
    return colors[type] || 'secondary';
}

function getPriorityColor(priority) {
    const colors = {
        'high': 'danger',
        'medium': 'warning',
        'low': 'info',
        'default': 'secondary'
    };
    return colors[priority] || 'secondary';
}

// ======================================== //
// ФУНКЦИИ ДЛЯ ИМПОРТА
// ======================================== //

function previewImport(file, rules) {
    showLoading('Анализ файла...');
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('rules', JSON.stringify(rules));
    
    $.ajax({
        url: '/api/import/preview',
        method: 'POST',
        data: formData,
        processData: false,
        contentType: false,
        success: function(response) {
            hideLoading();
            displayPreview(response);
        },
        error: function(xhr) {
            hideLoading();
            API.handleError(xhr);
        }
    });
}

function displayPreview(data) {
    const container = $('#previewContainer');
    if (!container.length) return;
    
    let html = '<div class="preview-table">';
    html += '<table class="table table-sm">';
    
    // Заголовки
    if (data.headers) {
        html += '<thead><tr>';
        data.headers.forEach(header => {
            html += `<th>${header}</th>`;
        });
        html += '</tr></thead>';
    }
    
    // Данные
    if (data.rows) {
        html += '<tbody>';
        data.rows.forEach(row => {
            const rowClass = row.valid ? 'preview-row-success' : 'preview-row-error';
            html += `<tr class="${rowClass}">`;
            row.cells.forEach(cell => {
                html += `<td>${cell}</td>`;
            });
            html += '</tr>';
        });
        html += '</tbody>';
    }
    
    html += '</table></div>';
    
    // Статистика
    if (data.stats) {
        html += `
            <div class="mt-3">
                <span class="badge bg-success">Готово: ${data.stats.valid}</span>
                <span class="badge bg-danger">Ошибки: ${data.stats.invalid}</span>
                <span class="badge bg-warning">Предупреждения: ${data.stats.warnings}</span>
            </div>
        `;
    }
    
    container.html(html);
}

// Экспортируем функции в глобальную область
window.API = API;
window.showAlert = showAlert;
window.showToast = showToast;
window.showLoading = showLoading;
window.hideLoading = hideLoading;
window.formatNumber = formatNumber;
window.formatCurrency = formatCurrency;
window.formatDate = formatDate;
window.formatDateTime = formatDateTime;
// ======================================== //
// UNSAVED CHANGES GUARD
// Предупреждает пользователя о несохранённых изменениях при попытке
// покинуть страницу. Активируется на формах с классом .unsaved-guard
// или вручную через new UnsavedChangesGuard(form).
// ======================================== //

class UnsavedChangesGuard {
    /**
     * @param {HTMLFormElement|string} formOrSelector - форма или CSS-селектор
     * @param {string} [message] - текст предупреждения (браузер может его игнорировать)
     */
    constructor(formOrSelector, message) {
        const form = typeof formOrSelector === 'string'
            ? document.querySelector(formOrSelector)
            : formOrSelector;

        if (!form) return;

        this._form    = form;
        this._dirty   = false;
        this._message = message || 'Есть несохранённые изменения. Покинуть страницу?';
        this._onBeforeUnload = this._onBeforeUnload.bind(this);
        this._onInput        = this._onInput.bind(this);
        this._onSubmit       = this._onSubmit.bind(this);

        form.addEventListener('input',  this._onInput);
        form.addEventListener('change', this._onInput);
        form.addEventListener('submit', this._onSubmit);
        window.addEventListener('beforeunload', this._onBeforeUnload);
    }

    _onInput()  { this._dirty = true; }
    _onSubmit() { this._dirty = false; }

    _onBeforeUnload(e) {
        if (!this._dirty) return;
        e.preventDefault();
        e.returnValue = this._message;
        return this._message;
    }

    /** Сбросить флаг изменений (например, после автосохранения или AJAX-submit) */
    markClean() { this._dirty = false; }

    /** Явно пометить форму как «грязную» */
    markDirty() { this._dirty = true; }

    /**
     * Обертка для AJAX-отправки: снимает dirty перед fetch,
     * восстанавливает если запрос провалился.
     * @param {Promise} fetchPromise
     * @returns {Promise}
     */
    submitViaAjax(fetchPromise) {
        this._dirty = false;
        return fetchPromise.catch(err => {
            this._dirty = true;
            throw err;
        });
    }

    /** Удалить все обработчики */
    destroy() {
        this._form.removeEventListener('input',  this._onInput);
        this._form.removeEventListener('change', this._onInput);
        this._form.removeEventListener('submit', this._onSubmit);
        window.removeEventListener('beforeunload', this._onBeforeUnload);
    }
}

// Автоматически навешиваем на формы с классом .unsaved-guard
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('form.unsaved-guard').forEach(form => {
        new UnsavedChangesGuard(form);
    });
});

window.UnsavedChangesGuard = UnsavedChangesGuard;

window.validateForm = validateForm;
window.validateEmail = validateEmail;
window.validatePhone = validatePhone;
window.uploadFile = uploadFile;
window.exportData = exportData;
window.copyToClipboard = copyToClipboard;
window.getStatusColor = getStatusColor;
window.getStatusIcon = getStatusIcon;
window.confirmLegacy = confirmLegacy;
window.loadCategoryRules = loadCategoryRules;
window.previewImport = previewImport;
window.debounce = debounce;