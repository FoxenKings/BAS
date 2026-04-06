"""
Скрипт заполнения справочника категорий.

Запуск:
    python seed_categories.py              # режим по умолчанию: добавить если пусто
    python seed_categories.py --force      # очистить и заполнить заново
    python seed_categories.py --check      # только проверить, ничего не менять

Иерархия (вложенные множества, Nested Set):
    Уровень 0: ASSETS, INVENTORY, SERVICES, INTANGIBLE
    Уровень 1: подразделы (IT, инструмент, материалы, СИЗ …)
    Уровень 2: группы (станки, электроинструмент, металлопрокат …)
    Уровень 3: подгруппы (сверла, листовой прокат …)
"""
import sqlite3
import os
import sys
import argparse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Путь к базе данных
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, 'data', 'assets.db')

# ------------------------------------------------------------------
# Полный список категорий
# Формат строки: (id, code, name_ru, parent_id, level, type,
#                 accounting_type, account_method, sort_order,
#                 lft, rgt, path, description, is_active)
# ------------------------------------------------------------------
CATEGORIES = [
    # ================================================================
    # Уровень 0: Корневые категории
    # ================================================================
    (1,  'ASSETS',    'Основные средства',               None, 0, 'asset',     'asset',      'individual',   100,   1,  180, 'ASSETS',    'Основные средства предприятия',               1),
    (2,  'INVENTORY', 'Товарно-материальные ценности',   None, 0, 'material',  'inventory',  'mixed',        200, 181,  660, 'INVENTORY', 'Товарно-материальные ценности',               1),
    (3,  'SERVICES',  'Услуги и работы',                 None, 0, 'consumable','service',    'batch',        300, 661,  680, 'SERVICES',  'Услуги и работы сторонних организаций',       1),
    (4,  'INTANGIBLE','Нематериальные активы',           None, 0, 'asset',     'intangible', 'individual',   400, 681,  700, 'INTANGIBLE','Нематериальные активы',                       1),

    # ================================================================
    # Уровень 1: ОСНОВНЫЕ СРЕДСТВА (ASSETS)
    # ================================================================
    (5,  'ASSETS_IT',       'IT и телекоммуникации',                     1, 1, 'asset',    'asset',   'individual', 110,   2,  35, 'ASSETS/ASSETS_IT',        'IT оборудование и телекоммуникации',            1),
    (6,  'ASSETS_PROD',     'Производственное оборудование',             1, 1, 'equipment','asset',   'individual', 120,  36,  85, 'ASSETS/ASSETS_PROD',       'Производственное оборудование',                 1),
    (7,  'ASSETS_TRANSP',   'Транспортные средства',                     1, 1, 'asset',    'asset',   'individual', 130,  86, 105, 'ASSETS/ASSETS_TRANSP',     'Транспортные средства',                         1),
    (8,  'ASSETS_BUILDING', 'Здания и сооружения',                       1, 1, 'asset',    'asset',   'individual', 140, 106, 115, 'ASSETS/ASSETS_BUILDING',   'Здания и сооружения',                           1),
    (9,  'ASSETS_FURN',     'Мебель и оснащение',                        1, 1, 'asset',    'asset',   'individual', 150, 116, 125, 'ASSETS/ASSETS_FURN',       'Мебель и оснащение',                            1),
    (10, 'ASSETS_MEASURE',  'Метрологическое и испытательное',           1, 1, 'equipment','asset',   'individual', 160, 126, 145, 'ASSETS/ASSETS_MEASURE',    'Метрологическое и испытательное оборудование',  1),
    (11, 'ASSETS_SPECIAL',  'Спецтехника',                               1, 1, 'equipment','asset',   'individual', 170, 146, 155, 'ASSETS/ASSETS_SPECIAL',    'Специальная техника',                           1),
    (12, 'ASSETS_3D_PRINTER','3D-принтеры и аддитивное оборудование',   1, 1, 'equipment','asset',   'individual', 175, 156, 165, 'ASSETS/ASSETS_3D_PRINTER', '3D-принтеры FDM/LCD/SLA',                       1),
    (13, 'ASSETS_UAV',      'Беспилотные воздушные суда (БАС)',         1, 1, 'asset',    'asset',   'individual', 180, 166, 180, 'ASSETS/ASSETS_UAV',        'Беспилотные воздушные суда и комплектующие',    1),

    # ================================================================
    # Уровень 2: IT и телекоммуникации (ASSETS_IT)
    # ================================================================
    (14, 'ASSETS_IT_COMP',   'Компьютеры и рабочие станции', 5, 2, 'asset','asset','individual', 10,  3,  6, 'ASSETS/ASSETS_IT/ASSETS_IT_COMP',   'Компьютеры, ноутбуки, моноблоки',          1),
    (15, 'ASSETS_IT_SERVER', 'Серверы и системы хранения',   5, 2, 'asset','asset','individual', 20,  7, 10, 'ASSETS/ASSETS_IT/ASSETS_IT_SERVER', 'Серверное оборудование и СХД',              1),
    (16, 'ASSETS_IT_NET',    'Сетевое оборудование',         5, 2, 'asset','asset','individual', 30, 11, 14, 'ASSETS/ASSETS_IT/ASSETS_IT_NET',    'Коммутаторы, маршрутизаторы, точки доступа',1),
    (17, 'ASSETS_IT_PRINT',  'Печатное оборудование',        5, 2, 'asset','asset','individual', 40, 15, 18, 'ASSETS/ASSETS_IT/ASSETS_IT_PRINT',  'Принтеры, МФУ, плоттеры',                  1),
    (18, 'ASSETS_IT_MOBILE', 'Мобильные устройства',         5, 2, 'asset','asset','individual', 50, 19, 22, 'ASSETS/ASSETS_IT/ASSETS_IT_MOBILE', 'Смартфоны, планшеты',                       1),
    (19, 'ASSETS_IT_AV',     'Аудио/видео оборудование',     5, 2, 'asset','asset','individual', 60, 23, 26, 'ASSETS/ASSETS_IT/ASSETS_IT_AV',     'Мониторы, проекторы, аудиосистемы',         1),
    (20, 'ASSETS_IT_UPS',    'ИБП и электропитание',         5, 2, 'asset','asset','individual', 70, 27, 30, 'ASSETS/ASSETS_IT/ASSETS_IT_UPS',    'Источники бесперебойного питания',          1),
    (21, 'ASSETS_IT_ANT',    'Антенны и радиооборудование',  5, 2, 'asset','asset','individual', 80, 31, 34, 'ASSETS/ASSETS_IT/ASSETS_IT_ANT',    'Антенны, радиостанции',                     1),

    # ================================================================
    # Уровень 2: Производственное оборудование (ASSETS_PROD)
    # ================================================================
    (22, 'ASSETS_PROD_CNC',   'Станки с ЧПУ',               6, 2, 'equipment','asset','individual', 10, 37, 40, 'ASSETS/ASSETS_PROD/ASSETS_PROD_CNC',   'Станки с числовым программным управлением', 1),
    (23, 'ASSETS_PROD_MACH',  'Универсальные станки',        6, 2, 'equipment','asset','individual', 20, 41, 44, 'ASSETS/ASSETS_PROD/ASSETS_PROD_MACH',  'Токарные, фрезерные станки',                1),
    (24, 'ASSETS_PROD_WELD',  'Сварочное оборудование',      6, 2, 'equipment','asset','individual', 30, 45, 48, 'ASSETS/ASSETS_PROD/ASSETS_PROD_WELD',  'Сварочные аппараты',                        1),
    (25, 'ASSETS_PROD_3D',    '3D-принтеры промышленные',    6, 2, 'equipment','asset','individual', 35, 49, 52, 'ASSETS/ASSETS_PROD/ASSETS_PROD_3D',    'Промышленные 3D-принтеры',                  1),
    (26, 'ASSETS_PROD_LASER', 'Лазерное оборудование',       6, 2, 'equipment','asset','individual', 40, 53, 56, 'ASSETS/ASSETS_PROD/ASSETS_PROD_LASER', 'Лазерные станки, граверы',                  1),
    (27, 'ASSETS_PROD_HYD',   'Гидравлическое оборудование', 6, 2, 'equipment','asset','individual', 50, 57, 60, 'ASSETS/ASSETS_PROD/ASSETS_PROD_HYD',   'Гидравлические прессы, насосы',             1),
    (28, 'ASSETS_PROD_LIFT',  'Подъёмное оборудование',      6, 2, 'equipment','asset','individual', 60, 61, 64, 'ASSETS/ASSETS_PROD/ASSETS_PROD_LIFT',  'Краны, тельферы, лебедки',                  1),
    (29, 'ASSETS_PROD_COMP',  'Компрессоры и пневматика',    6, 2, 'equipment','asset','individual', 70, 65, 68, 'ASSETS/ASSETS_PROD/ASSETS_PROD_COMP',  'Воздушные компрессоры',                     1),
    (30, 'ASSETS_PROD_HEAT',  'Термическое оборудование',    6, 2, 'equipment','asset','individual', 80, 69, 72, 'ASSETS/ASSETS_PROD/ASSETS_PROD_HEAT',  'Печи, термокамеры',                         1),
    (31, 'ASSETS_PROD_COAT',  'Окрасочное оборудование',     6, 2, 'equipment','asset','individual', 90, 73, 76, 'ASSETS/ASSETS_PROD/ASSETS_PROD_COAT',  'Краскопульты, окрасочные камеры',           1),
    (32, 'ASSETS_PROD_TEST',  'Испытательное оборудование',  6, 2, 'equipment','asset','individual',100, 77, 80, 'ASSETS/ASSETS_PROD/ASSETS_PROD_TEST',  'Стенды, испытательные комплексы',           1),
    (33, 'ASSETS_PROD_PRESS', 'Прессовое и кузнечное',       6, 2, 'equipment','asset','individual',110, 81, 84, 'ASSETS/ASSETS_PROD/ASSETS_PROD_PRESS', 'Прессы, молоты',                            1),

    # ================================================================
    # Уровень 2: Транспортные средства (ASSETS_TRANSP)
    # ================================================================
    (34, 'ASSETS_TR_CAR',  'Легковые автомобили',     7, 2, 'asset','asset','individual', 10,  87,  90, 'ASSETS/ASSETS_TRANSP/ASSETS_TR_CAR',  'Легковой автотранспорт',  1),
    (35, 'ASSETS_TR_TRUCK','Грузовые автомобили',     7, 2, 'asset','asset','individual', 20,  91,  94, 'ASSETS/ASSETS_TRANSP/ASSETS_TR_TRUCK','Грузовой автотранспорт',  1),
    (36, 'ASSETS_TR_BUS',  'Автобусы и микроавтобусы',7, 2, 'asset','asset','individual', 30,  95,  98, 'ASSETS/ASSETS_TRANSP/ASSETS_TR_BUS',  'Пассажирский транспорт',  1),
    (37, 'ASSETS_TR_FORK', 'Погрузчики и штабелёры', 7, 2, 'asset','asset','individual', 40,  99, 102, 'ASSETS/ASSETS_TRANSP/ASSETS_TR_FORK', 'Складская техника',       1),
    (38, 'ASSETS_TR_SPEC', 'Специальный транспорт',  7, 2, 'asset','asset','individual', 50, 103, 104, 'ASSETS/ASSETS_TRANSP/ASSETS_TR_SPEC', 'Спецтехника на шасси',   1),

    # ================================================================
    # Уровень 2: Здания и сооружения (ASSETS_BUILDING)
    # ================================================================
    (39, 'ASSETS_BLD_BUILD', 'Здания',           8, 2, 'asset','asset','individual', 10, 107, 110, 'ASSETS/ASSETS_BUILDING/ASSETS_BLD_BUILD', 'Производственные и административные здания', 1),
    (40, 'ASSETS_BLD_STRUCT','Сооружения',       8, 2, 'asset','asset','individual', 20, 111, 112, 'ASSETS/ASSETS_BUILDING/ASSETS_BLD_STRUCT','Инженерные сооружения',                       1),
    (41, 'ASSETS_BLD_LAND',  'Земельные участки',8, 2, 'asset','asset','individual', 30, 113, 114, 'ASSETS/ASSETS_BUILDING/ASSETS_BLD_LAND',  'Земельные участки',                           1),

    # ================================================================
    # Уровень 2: Мебель и оснащение (ASSETS_FURN)
    # ================================================================
    (42, 'ASSETS_FURN_DESK', 'Столы и тумбы',       9, 2, 'asset','asset','individual', 10, 117, 118, 'ASSETS/ASSETS_FURN/ASSETS_FURN_DESK', 'Рабочие столы, тумбы',              1),
    (43, 'ASSETS_FURN_SEAT', 'Кресла и стулья',     9, 2, 'asset','asset','individual', 20, 119, 120, 'ASSETS/ASSETS_FURN/ASSETS_FURN_SEAT', 'Офисные кресла, стулья',            1),
    (44, 'ASSETS_FURN_CAB',  'Шкафы и стеллажи',   9, 2, 'asset','asset','individual', 30, 121, 122, 'ASSETS/ASSETS_FURN/ASSETS_FURN_CAB',  'Шкафы, стеллажи, системы хранения', 1),
    (45, 'ASSETS_FURN_OTHER','Прочая мебель',       9, 2, 'asset','asset','individual', 40, 123, 124, 'ASSETS/ASSETS_FURN/ASSETS_FURN_OTHER','Прочие мебельные изделия',           1),

    # ================================================================
    # Уровень 2: Метрологическое оборудование (ASSETS_MEASURE)
    # ================================================================
    (46, 'ASSETS_MEAS_DIM',  'Измерение геометрических величин',10, 2, 'equipment','asset','individual', 10, 127, 130, 'ASSETS/ASSETS_MEASURE/ASSETS_MEAS_DIM',  'Координатно-измерительные машины', 1),
    (47, 'ASSETS_MEAS_FORCE','Силоизмерительное оборудование',  10, 2, 'equipment','asset','individual', 20, 131, 134, 'ASSETS/ASSETS_MEASURE/ASSETS_MEAS_FORCE','Динамометры, весы крановые',       1),
    (48, 'ASSETS_MEAS_ELEC', 'Электроизмерительное оборудование',10,2, 'equipment','asset','individual', 30, 135, 138, 'ASSETS/ASSETS_MEASURE/ASSETS_MEAS_ELEC', 'Осциллографы, мультиметры',        1),
    (49, 'ASSETS_MEAS_THERM','Температурное оборудование',      10, 2, 'equipment','asset','individual', 40, 139, 140, 'ASSETS/ASSETS_MEASURE/ASSETS_MEAS_THERM','Пирометры, термометры',            1),
    (50, 'ASSETS_MEAS_OPT',  'Оптическое оборудование',         10, 2, 'equipment','asset','individual', 50, 141, 144, 'ASSETS/ASSETS_MEASURE/ASSETS_MEAS_OPT',  'Микроскопы, эндоскопы',            1),

    # ================================================================
    # Уровень 2: Спецтехника (ASSETS_SPECIAL)
    # ================================================================
    (51, 'ASSETS_SP_CRANE','Краны и подъёмники',   11, 2, 'equipment','asset','individual', 10, 147, 148, 'ASSETS/ASSETS_SPECIAL/ASSETS_SP_CRANE','Автокраны, подъемники', 1),
    (52, 'ASSETS_SP_EARTH','Землеройная техника',  11, 2, 'equipment','asset','individual', 20, 149, 150, 'ASSETS/ASSETS_SPECIAL/ASSETS_SP_EARTH','Экскаваторы, бульдозеры',1),
    (53, 'ASSETS_SP_AGRO', 'Сельхозтехника',       11, 2, 'equipment','asset','individual', 30, 151, 154, 'ASSETS/ASSETS_SPECIAL/ASSETS_SP_AGRO', 'Тракторы, комбайны',    1),

    # ================================================================
    # Уровень 1: ИНВЕНТАРЬ (INVENTORY) — основные разделы
    # ================================================================
    (54, 'INV_TOOLS',      'Инструмент',                        2, 1, 'tool',      'inventory','mixed',        210, 182, 281, 'INVENTORY/INV_TOOLS',      'Ручной и механизированный инструмент',     1),
    (55, 'INV_MATERIALS',  'Материалы и сырьё',                 2, 1, 'material',  'inventory','quantitative', 220, 282, 325, 'INVENTORY/INV_MATERIALS',  'Сырьё и основные материалы',               1),
    (56, 'INV_CONSUMABLES','Расходные материалы',               2, 1, 'consumable','inventory','quantitative', 230, 326, 395, 'INVENTORY/INV_CONSUMABLES','Расходные материалы для производства',     1),
    (57, 'INV_PPE',        'Средства индивидуальной защиты',    2, 1, 'ppe',       'inventory','individual',   240, 396, 435, 'INVENTORY/INV_PPE',        'СИЗ и спецодежда',                         1),
    (58, 'INV_ELECTRONICS','Электронные компоненты',            2, 1, 'consumable','inventory','quantitative', 250, 436, 481, 'INVENTORY/INV_ELECTRONICS','Электронные компоненты и модули',           1),
    (59, 'INV_CHEMICALS',  'Химические вещества и ГСМ',        2, 1, 'material',  'inventory','batch',        260, 482, 511, 'INVENTORY/INV_CHEMICALS',  'Химия, топливо, смазки',                   1),
    (60, 'INV_AUTO',       'Автомобильные запчасти',            2, 1, 'material',  'inventory','quantitative', 270, 512, 545, 'INVENTORY/INV_AUTO',       'Запчасти для автотранспорта',               1),
    (61, 'INV_AVIATION',   'Авиационные и БАС компоненты',      2, 1, 'material',  'inventory','quantitative', 280, 546, 585, 'INVENTORY/INV_AVIATION',   'Комплектующие для БАС и авиации',           1),
    (62, 'INV_OFFICE',     'Офисные и хозяйственные товары',   2, 1, 'consumable','inventory','quantitative', 290, 586, 605, 'INVENTORY/INV_OFFICE',     'Канцелярия, хозтовары',                    1),
    (63, 'INV_PACKAGING',  'Упаковка и тара',                   2, 1, 'consumable','inventory','quantitative', 300, 606, 625, 'INVENTORY/INV_PACKAGING',  'Упаковочные материалы',                    1),
    # ------- Дополнительные разделы (выявлены по списку номенклатуры) -------
    (64, 'INV_FOOD',       'Продукты питания и напитки',        2, 1, 'consumable','inventory','quantitative', 310, 626, 635, 'INVENTORY/INV_FOOD',       'Продукты, вода, напитки, сладости',        1),
    (65, 'INV_MEDICAL',    'Медицина и первая помощь',          2, 1, 'consumable','inventory','quantitative', 320, 636, 645, 'INVENTORY/INV_MEDICAL',    'Аптечки, медикаменты, перевязочные',       1),
    (66, 'INV_OTHER',      'Прочие МЦ',                        2, 1, 'material',  'inventory','quantitative', 390, 646, 655, 'INVENTORY/INV_OTHER',      'Прочие материальные ценности',             1),

    # ================================================================
    # Уровень 2: ИНСТРУМЕНТ (INV_TOOLS)
    # ================================================================
    (67, 'INV_TOOLS_HAND', 'Ручной инструмент',       54, 2, 'tool','inventory','individual', 10, 183, 208, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND', 'Ручной слесарно-монтажный инструмент',          1),
    (68, 'INV_TOOLS_POWER','Электроинструмент',        54, 2, 'tool','inventory','individual', 20, 209, 226, 'INVENTORY/INV_TOOLS/INV_TOOLS_POWER','Аккумуляторный и сетевой электроинструмент',    1),
    (69, 'INV_TOOLS_PNEUM','Пневмоинструмент',         54, 2, 'tool','inventory','individual', 30, 227, 230, 'INVENTORY/INV_TOOLS/INV_TOOLS_PNEUM','Пневматический инструмент',                      1),
    (70, 'INV_TOOLS_MEAS', 'Измерительный инструмент', 54, 2, 'tool','inventory','individual', 40, 231, 264, 'INVENTORY/INV_TOOLS/INV_TOOLS_MEAS', 'Измерительные приборы и инструменты',           1),
    (71, 'INV_TOOLS_CUT',  'Режущий инструмент',       54, 2, 'tool','inventory','quantitative',50,265, 276, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT',  'Сверла, фрезы, метчики',                        1),
    (72, 'INV_TOOLS_CNC',  'Оснастка для ЧПУ',        54, 2, 'tool','inventory','individual', 60, 277, 280, 'INVENTORY/INV_TOOLS/INV_TOOLS_CNC',  'Цанги, патроны, держатели для ЧПУ',             1),

    # Уровень 3: Ручной инструмент
    (73, 'INV_TH_WRENCH', 'Ключи',                     67, 3, 'tool','inventory','individual', 10, 184, 187, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_WRENCH', 'Гаечные, рожковые, торцевые ключи',  1),
    (74, 'INV_TH_SCREW',  'Отвёртки',                  67, 3, 'tool','inventory','individual', 20, 188, 191, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_SCREW',  'Отвёртки различных типов',            1),
    (75, 'INV_TH_PLIERS', 'Плоскогубцы и кусачки',     67, 3, 'tool','inventory','individual', 30, 192, 195, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_PLIERS', 'Пассатижи, кусачки, бокорезы',       1),
    (76, 'INV_TH_HAMMER', 'Молотки и кувалды',          67, 3, 'tool','inventory','individual', 40, 196, 199, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_HAMMER', 'Молотки, киянки, кувалды',           1),
    (77, 'INV_TH_SAW',    'Ножовки и пилы',             67, 3, 'tool','inventory','individual', 50, 200, 201, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_SAW',    'Ручные пилы, ножовки',               1),
    (78, 'INV_TH_CLAMP',  'Зажимы и струбцины',         67, 3, 'tool','inventory','individual', 60, 202, 203, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_CLAMP',  'Тиски, струбцины, зажимы',           1),
    (79, 'INV_TH_OTHER',  'Прочий ручной инструмент',   67, 3, 'tool','inventory','individual', 70, 204, 207, 'INVENTORY/INV_TOOLS/INV_TOOLS_HAND/INV_TH_OTHER',  'Зубила, шаберы, монтажки',           1),

    # Уровень 3: Электроинструмент
    (80, 'INV_TP_DRILL', 'Дрели и шуруповёрты',     68, 3, 'tool','inventory','individual', 10, 210, 213, 'INVENTORY/INV_TOOLS/INV_TOOLS_POWER/INV_TP_DRILL', 'Дрели, шуруповерты',                1),
    (81, 'INV_TP_GRIND', 'Шлифмашины',              68, 3, 'tool','inventory','individual', 20, 214, 217, 'INVENTORY/INV_TOOLS/INV_TOOLS_POWER/INV_TP_GRIND', 'УШМ, шлифовальные машины',          1),
    (82, 'INV_TP_SAW',   'Электрические пилы',      68, 3, 'tool','inventory','individual', 30, 218, 221, 'INVENTORY/INV_TOOLS/INV_TOOLS_POWER/INV_TP_SAW',   'Электролобзики, циркулярные пилы',  1),
    (83, 'INV_TP_OTHER', 'Прочий электроинструмент',68, 3, 'tool','inventory','individual', 40, 222, 225, 'INVENTORY/INV_TOOLS/INV_TOOLS_POWER/INV_TP_OTHER', 'Реноваторы, фрезеры',               1),

    # Уровень 3: Измерительный инструмент
    (84, 'INV_TM_LIN',  'Линейные измерения',    70, 3, 'tool',     'inventory','individual', 10, 232, 241, 'INVENTORY/INV_TOOLS/INV_TOOLS_MEAS/INV_TM_LIN',  'Штангенциркули, микрометры, линейки', 1),
    (85, 'INV_TM_ANG',  'Угловые измерения',     70, 3, 'tool',     'inventory','individual', 20, 242, 245, 'INVENTORY/INV_TOOLS/INV_TOOLS_MEAS/INV_TM_ANG',  'Угломеры, уровни',                   1),
    (86, 'INV_TM_ELEC', 'Электрические измерения',70,3, 'equipment','asset',    'individual', 30, 246, 255, 'INVENTORY/INV_TOOLS/INV_TOOLS_MEAS/INV_TM_ELEC', 'Мультиметры, осциллографы, мегаомметры',1),
    (87, 'INV_TM_OTHER','Прочие измерения',      70, 3, 'tool',     'inventory','individual', 40, 256, 263, 'INVENTORY/INV_TOOLS/INV_TOOLS_MEAS/INV_TM_OTHER', 'Тахометры, щупы, шаблоны',           1),

    # Уровень 3: Режущий инструмент
    (88, 'INV_TC_DRILL', 'Свёрла',             71, 3, 'tool','inventory','quantitative', 10, 266, 267, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT/INV_TC_DRILL', 'Спиральные сверла',         1),
    (89, 'INV_TC_MILL',  'Фрезы',              71, 3, 'tool','inventory','quantitative', 20, 268, 269, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT/INV_TC_MILL',  'Концевые, торцевые фрезы',  1),
    (90, 'INV_TC_TURN',  'Резцы',              71, 3, 'tool','inventory','quantitative', 30, 270, 271, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT/INV_TC_TURN',  'Токарные резцы, пластины',  1),
    (91, 'INV_TC_THREAD','Метчики и плашки',   71, 3, 'tool','inventory','quantitative', 40, 272, 273, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT/INV_TC_THREAD','Метчики, плашки',           1),
    (92, 'INV_TC_ABRAS', 'Абразивный инструмент',71,3,'tool','inventory','quantitative', 50, 274, 275, 'INVENTORY/INV_TOOLS/INV_TOOLS_CUT/INV_TC_ABRAS', 'Шлифовальные круги, диски', 1),

    # ================================================================
    # Уровень 2: МАТЕРИАЛЫ (INV_MATERIALS)
    # ================================================================
    (93,  'INV_MAT_METAL',    'Металлопрокат',        55, 2, 'material','inventory','quantitative', 10, 283, 300, 'INVENTORY/INV_MATERIALS/INV_MAT_METAL',    'Черный и цветной металлопрокат', 1),
    (94,  'INV_MAT_POLYMER',  'Полимеры и пластики',  55, 2, 'material','inventory','quantitative', 20, 301, 304, 'INVENTORY/INV_MATERIALS/INV_MAT_POLYMER',  'Пластики, полимеры',             1),
    (95,  'INV_MAT_WOOD',     'Древесина',             55, 2, 'material','inventory','quantitative', 30, 305, 308, 'INVENTORY/INV_MATERIALS/INV_MAT_WOOD',     'Пиломатериалы, фанера',          1),
    (96,  'INV_MAT_COMPOSITE','Композиты',             55, 2, 'material','inventory','quantitative', 40, 309, 312, 'INVENTORY/INV_MATERIALS/INV_MAT_COMPOSITE','Стеклопластик, карбон',          1),
    (97,  'INV_MAT_RUBBER',   'Резина',                55, 2, 'material','inventory','quantitative', 50, 313, 316, 'INVENTORY/INV_MATERIALS/INV_MAT_RUBBER',   'Резина, эластомеры',             1),
    (98,  'INV_MAT_TEXTILE',  'Ткани',                 55, 2, 'material','inventory','quantitative', 60, 317, 320, 'INVENTORY/INV_MATERIALS/INV_MAT_TEXTILE',  'Текстильные материалы',          1),
    (99,  'INV_MAT_CABLE',    'Кабели и провода',      55, 2, 'material','inventory','quantitative', 70, 321, 324, 'INVENTORY/INV_MATERIALS/INV_MAT_CABLE',    'Кабельная продукция',            1),

    # Уровень 3: Металлопрокат
    (100, 'INV_MM_SHEET',  'Листовой прокат', 93, 3, 'material','inventory','quantitative', 10, 284, 287, 'INVENTORY/INV_MATERIALS/INV_MAT_METAL/INV_MM_SHEET',  'Листы, плиты',                    1),
    (101, 'INV_MM_PIPE',   'Трубы',           93, 3, 'material','inventory','quantitative', 20, 288, 291, 'INVENTORY/INV_MATERIALS/INV_MAT_METAL/INV_MM_PIPE',   'Трубы круглые, профильные',       1),
    (102, 'INV_MM_PROFILE','Профили',         93, 3, 'material','inventory','quantitative', 30, 292, 295, 'INVENTORY/INV_MATERIALS/INV_MAT_METAL/INV_MM_PROFILE','Уголки, швеллеры, балки',         1),
    (103, 'INV_MM_ROD',    'Прутки',          93, 3, 'material','inventory','quantitative', 40, 296, 299, 'INVENTORY/INV_MATERIALS/INV_MAT_METAL/INV_MM_ROD',    'Круги, шестигранники, проволока', 1),

    # ================================================================
    # Уровень 2: РАСХОДНЫЕ МАТЕРИАЛЫ (INV_CONSUMABLES)
    # ================================================================
    (104, 'INV_CONS_FAST',       'Крепёж',                  56, 2, 'consumable','inventory','quantitative', 10, 327, 330, 'INVENTORY/INV_CONSUMABLES/INV_CONS_FAST',      'Болты, винты, гайки, шайбы',      1),
    (105, 'INV_CONS_ABRAS',      'Абразивы',                56, 2, 'consumable','inventory','quantitative', 20, 331, 334, 'INVENTORY/INV_CONSUMABLES/INV_CONS_ABRAS',     'Шлифовальные шкурки, губки',      1),
    (106, 'INV_CONS_LUB',        'Смазки',                  56, 2, 'consumable','inventory','batch',        30, 335, 338, 'INVENTORY/INV_CONSUMABLES/INV_CONS_LUB',       'Смазочные материалы',             1),
    (107, 'INV_CONS_ADH',        'Клеи и герметики',        56, 2, 'consumable','inventory','batch',        40, 339, 344, 'INVENTORY/INV_CONSUMABLES/INV_CONS_ADH',       'Клеи, герметики, эпоксидки',      1),
    (108, 'INV_CONS_WELD',       'Сварочные расходники',    56, 2, 'consumable','inventory','batch',        50, 345, 348, 'INVENTORY/INV_CONSUMABLES/INV_CONS_WELD',      'Электроды, присадочная проволока',1),
    (109, 'INV_CONS_3D',         'Расходники для 3D-печати',56, 2, 'consumable','inventory','quantitative', 55, 349, 358, 'INVENTORY/INV_CONSUMABLES/INV_CONS_3D',        'Пластик, смола для 3D-печати',    1),
    (110, 'INV_CONS_3D_FILAMENT','Пластик для FDM-печати',  56, 2, 'consumable','inventory','quantitative', 56, 359, 362, 'INVENTORY/INV_CONSUMABLES/INV_CONS_3D_FILAMENT','PLA, ABS, PETG, нейлон',         1),
    (111, 'INV_CONS_3D_RESIN',   'Фотополимерные смолы',   56, 2, 'consumable','inventory','quantitative', 57, 363, 366, 'INVENTORY/INV_CONSUMABLES/INV_CONS_3D_RESIN',   'Смолы для LCD/DLP печати',        1),
    (112, 'INV_CONS_FILT',       'Фильтры',                56, 2, 'consumable','inventory','quantitative', 60, 367, 370, 'INVENTORY/INV_CONSUMABLES/INV_CONS_FILT',       'Воздушные, масляные фильтры',     1),
    (113, 'INV_CONS_CLEAN',      'Моющие средства',         56, 2, 'consumable','inventory','batch',        70, 371, 376, 'INVENTORY/INV_CONSUMABLES/INV_CONS_CLEAN',      'Моющие и чистящие средства',      1),
    (114, 'INV_CONS_PRINT',      'Расходники для печати',   56, 2, 'consumable','inventory','batch',        80, 377, 382, 'INVENTORY/INV_CONSUMABLES/INV_CONS_PRINT',      'Картриджи, тонеры',               1),
    (115, 'INV_CONS_COMPOSITE',  'Композитные материалы',   56, 2, 'material',  'inventory','quantitative', 90, 383, 388, 'INVENTORY/INV_CONSUMABLES/INV_CONS_COMPOSITE',  'Стекломат, ткань, смолы',         1),
    (116, 'INV_CONS_PAINT',      'Лакокрасочные материалы', 56, 2, 'consumable','inventory','batch',        95, 389, 394, 'INVENTORY/INV_CONSUMABLES/INV_CONS_PAINT',      'Краски, лаки, растворители',      1),

    # ================================================================
    # Уровень 2: СИЗ (INV_PPE)
    # ================================================================
    (117, 'INV_PPE_HEAD',    'Защита головы',          57, 2, 'ppe','inventory','individual', 10, 397, 398, 'INVENTORY/INV_PPE/INV_PPE_HEAD',    'Каски, шлемы',               1),
    (118, 'INV_PPE_EYE',     'Защита зрения',          57, 2, 'ppe','inventory','individual', 20, 399, 400, 'INVENTORY/INV_PPE/INV_PPE_EYE',     'Очки, маски',                1),
    (119, 'INV_PPE_HEAR',    'Защита слуха',           57, 2, 'ppe','inventory','individual', 30, 401, 402, 'INVENTORY/INV_PPE/INV_PPE_HEAR',    'Наушники, беруши',           1),
    (120, 'INV_PPE_RESP',    'Защита дыхания',         57, 2, 'ppe','inventory','individual', 40, 403, 404, 'INVENTORY/INV_PPE/INV_PPE_RESP',    'Респираторы, противогазы',   1),
    (121, 'INV_PPE_HAND',    'Защита рук',             57, 2, 'ppe','inventory','individual', 50, 405, 406, 'INVENTORY/INV_PPE/INV_PPE_HAND',    'Перчатки, рукавицы',         1),
    (122, 'INV_PPE_BODY',    'Защита тела',            57, 2, 'ppe','inventory','individual', 60, 407, 416, 'INVENTORY/INV_PPE/INV_PPE_BODY',    'Спецодежда, костюмы',        1),
    (123, 'INV_PPE_FOOT',    'Защита ног',             57, 2, 'ppe','inventory','individual', 70, 417, 418, 'INVENTORY/INV_PPE/INV_PPE_FOOT',    'Обувь защитная',             1),
    (124, 'INV_PPE_FALL',    'Защита от падений',      57, 2, 'ppe','inventory','individual', 80, 419, 420, 'INVENTORY/INV_PPE/INV_PPE_FALL',    'Страховочные системы',       1),
    (125, 'INV_PPE_TACTICAL','Тактическое снаряжение', 57, 2, 'ppe','inventory','individual', 90, 421, 434, 'INVENTORY/INV_PPE/INV_PPE_TACTICAL','Тактическая одежда, берцы',  1),

    # ================================================================
    # Уровень 2: ЭЛЕКТРОННЫЕ КОМПОНЕНТЫ (INV_ELECTRONICS)
    # ================================================================
    (126, 'INV_EL_PASSIVE',   'Пассивные компоненты',    58, 2, 'consumable','inventory','quantitative', 10, 437, 440, 'INVENTORY/INV_ELECTRONICS/INV_EL_PASSIVE',   'Резисторы, конденсаторы',                1),
    (127, 'INV_EL_ACTIVE',    'Активные компоненты',     58, 2, 'consumable','inventory','quantitative', 20, 441, 444, 'INVENTORY/INV_ELECTRONICS/INV_EL_ACTIVE',    'Транзисторы, диоды, микросхемы',         1),
    (128, 'INV_EL_DEVBOARD',  'Отладочные платы и МК',  58, 2, 'consumable','inventory','individual',   30, 445, 452, 'INVENTORY/INV_ELECTRONICS/INV_EL_DEVBOARD',  'Arduino, Raspberry Pi, ESP32',           1),
    (129, 'INV_EL_SENSOR',    'Датчики',                 58, 2, 'consumable','inventory','quantitative', 40, 453, 456, 'INVENTORY/INV_ELECTRONICS/INV_EL_SENSOR',    'Датчики температуры, тока, уровня',      1),
    (130, 'INV_EL_CONN',      'Разъёмы',                 58, 2, 'consumable','inventory','quantitative', 50, 457, 464, 'INVENTORY/INV_ELECTRONICS/INV_EL_CONN',      'Разъемы, клеммы, коннекторы',            1),
    (131, 'INV_EL_POWER_MOD', 'Модули питания',          58, 2, 'consumable','inventory','individual',   60, 465, 472, 'INVENTORY/INV_ELECTRONICS/INV_EL_POWER_MOD', 'DC-DC, BMS, реле, стабилизаторы',        1),
    (132, 'INV_EL_MODULE',    'Готовые модули',          58, 2, 'consumable','inventory','individual',   70, 473, 476, 'INVENTORY/INV_ELECTRONICS/INV_EL_MODULE',    'CAN шина, драйверы двигателей',          1),
    (133, 'INV_EL_DISPLAY',   'Дисплеи',                 58, 2, 'consumable','inventory','individual',   80, 477, 480, 'INVENTORY/INV_ELECTRONICS/INV_EL_DISPLAY',   'Экраны, индикаторы',                     1),

    # ================================================================
    # Уровень 2: ХИМИЧЕСКИЕ ВЕЩЕСТВА (INV_CHEMICALS)
    # ================================================================
    (134, 'INV_CHEM_GAS',  'Технические газы',  59, 2, 'material','inventory','quantitative', 10, 483, 486, 'INVENTORY/INV_CHEMICALS/INV_CHEM_GAS',  'Азот, кислород',            1),
    (135, 'INV_CHEM_LIQ',  'Жидкие химикаты',   59, 2, 'material','inventory','batch',        20, 487, 490, 'INVENTORY/INV_CHEMICALS/INV_CHEM_LIQ',  'Химические жидкости',       1),
    (136, 'INV_CHEM_SOLV', 'Растворители',       59, 2, 'material','inventory','batch',        30, 491, 494, 'INVENTORY/INV_CHEMICALS/INV_CHEM_SOLV', 'Растворители, разбавители', 1),
    (137, 'INV_CHEM_RESIN','Смолы',              59, 2, 'material','inventory','batch',        40, 495, 496, 'INVENTORY/INV_CHEMICALS/INV_CHEM_RESIN','Эпоксидные, полиэфирные смолы',1),
    (138, 'INV_CHEM_FUEL', 'Топливо',            59, 2, 'material','inventory','batch',        60, 497, 502, 'INVENTORY/INV_CHEMICALS/INV_CHEM_FUEL', 'Бензин, дизтопливо',        1),
    (139, 'INV_CHEM_COAT', 'Краски (ГСМ)',       59, 2, 'material','inventory','batch',        70, 503, 510, 'INVENTORY/INV_CHEMICALS/INV_CHEM_COAT', 'Краски, грунты, лаки (ГСМ)',1),

    # ================================================================
    # Уровень 2: АВТОЗАПЧАСТИ (INV_AUTO)
    # ================================================================
    (140, 'INV_AUTO_ENG',  'Двигатель и трансмиссия', 60, 2, 'material','inventory','quantitative', 10, 513, 516, 'INVENTORY/INV_AUTO/INV_AUTO_ENG',  'Двигатели, КПП',         1),
    (141, 'INV_AUTO_BODY', 'Кузов и оптика',           60, 2, 'material','inventory','quantitative', 20, 517, 520, 'INVENTORY/INV_AUTO/INV_AUTO_BODY', 'Кузовные детали, фары',  1),
    (142, 'INV_AUTO_ELEC', 'Электрика',                60, 2, 'material','inventory','quantitative', 30, 521, 524, 'INVENTORY/INV_AUTO/INV_AUTO_ELEC', 'Автоэлектрика, датчики', 1),
    (143, 'INV_AUTO_SUSP', 'Подвеска и рулевое',       60, 2, 'material','inventory','quantitative', 40, 525, 528, 'INVENTORY/INV_AUTO/INV_AUTO_SUSP', 'Амортизаторы, рычаги',   1),
    (144, 'INV_AUTO_BRAKE','Тормозная система',         60, 2, 'material','inventory','quantitative', 50, 529, 530, 'INVENTORY/INV_AUTO/INV_AUTO_BRAKE','Колодки, диски',         1),
    (145, 'INV_AUTO_WHEEL','Колёса и шины',             60, 2, 'material','inventory','quantitative', 60, 531, 534, 'INVENTORY/INV_AUTO/INV_AUTO_WHEEL','Шины, диски',            1),
    (146, 'INV_AUTO_COOL', 'Охлаждение и отопление',   60, 2, 'material','inventory','quantitative', 70, 535, 544, 'INVENTORY/INV_AUTO/INV_AUTO_COOL', 'Радиаторы, помпы',       1),

    # ================================================================
    # Уровень 2: БАС / АВИАЦИЯ (INV_AVIATION)
    # ================================================================
    (147, 'INV_UAV_FRAME',  'Рамы и корпуса БАС',    61, 2, 'material',  'inventory','individual',   10, 547, 550, 'INVENTORY/INV_AVIATION/INV_UAV_FRAME',  'Карбоновые и пластиковые рамы', 1),
    (148, 'INV_UAV_MOTOR',  'Моторы и приводы БАС',  61, 2, 'material',  'inventory','individual',   20, 551, 554, 'INVENTORY/INV_AVIATION/INV_UAV_MOTOR',  'Бесколлекторные моторы',        1),
    (149, 'INV_UAV_ESC',    'Регуляторы оборотов ESC',61,2, 'consumable','inventory','individual',   30, 555, 558, 'INVENTORY/INV_AVIATION/INV_UAV_ESC',    'ESC, регуляторы',               1),
    (150, 'INV_UAV_FC',     'Полетные контроллеры',  61, 2, 'consumable','inventory','individual',   40, 559, 562, 'INVENTORY/INV_AVIATION/INV_UAV_FC',     'FC, контроллеры полета',        1),
    (151, 'INV_UAV_PROP',   'Пропеллеры БАС',        61, 2, 'consumable','inventory','quantitative', 50, 563, 566, 'INVENTORY/INV_AVIATION/INV_UAV_PROP',   'Пропеллеры, винты',             1),
    (152, 'INV_UAV_BATT',   'Аккумуляторы БАС',      61, 2, 'consumable','inventory','individual',   60, 567, 570, 'INVENTORY/INV_AVIATION/INV_UAV_BATT',   'Li-Po аккумуляторы',            1),
    (153, 'INV_UAV_FPV',    'FPV-оборудование',      61, 2, 'consumable','inventory','individual',   70, 571, 576, 'INVENTORY/INV_AVIATION/INV_UAV_FPV',    'Очки, камеры, видеопередатчики',1),
    (154, 'INV_UAV_RADIO',  'Аппаратура управления', 61, 2, 'consumable','inventory','individual',   80, 577, 580, 'INVENTORY/INV_AVIATION/INV_UAV_RADIO',  'Пульты, приёмники',             1),
    (155, 'INV_UAV_PAYLOAD','Полезная нагрузка',      61, 2, 'consumable','inventory','individual',   90, 581, 584, 'INVENTORY/INV_AVIATION/INV_UAV_PAYLOAD','Камеры, сенсоры',               1),

    # ================================================================
    # Уровень 2: ОФИСНЫЕ / ХОЗЯЙСТВЕННЫЕ (INV_OFFICE)
    # ================================================================
    (156, 'INV_OFF_STAT', 'Канцелярия',          62, 2, 'consumable','inventory','quantitative', 10, 587, 590, 'INVENTORY/INV_OFFICE/INV_OFF_STAT', 'Ручки, карандаши',              1),
    (157, 'INV_OFF_PAPER','Бумажная продукция',  62, 2, 'consumable','inventory','quantitative', 20, 591, 594, 'INVENTORY/INV_OFFICE/INV_OFF_PAPER','Бумага, блокноты',               1),
    (158, 'INV_OFF_HYG',  'Хозяйственные товары',62, 2, 'consumable','inventory','quantitative', 30, 595, 604, 'INVENTORY/INV_OFFICE/INV_OFF_HYG',  'Мыло, салфетки, туалетная бумага',1),

    # ================================================================
    # Уровень 2: УПАКОВКА (INV_PACKAGING)
    # ================================================================
    (159, 'INV_PACK_BOX', 'Тара и коробки',   63, 2, 'consumable','inventory','quantitative', 10, 607, 610, 'INVENTORY/INV_PACKAGING/INV_PACK_BOX', 'Коробки, ящики',                    1),
    (160, 'INV_PACK_FILM','Плёнки',            63, 2, 'consumable','inventory','quantitative', 20, 611, 614, 'INVENTORY/INV_PACKAGING/INV_PACK_FILM','Стрейч-пленка, пузырчатая пленка',  1),
    (161, 'INV_PACK_TAPE','Клейкие ленты',     63, 2, 'consumable','inventory','quantitative', 30, 615, 624, 'INVENTORY/INV_PACKAGING/INV_PACK_TAPE','Скотч, малярная лента, термоскотч', 1),

    # ================================================================
    # Уровень 2: ПРОДУКТЫ ПИТАНИЯ (INV_FOOD) — выявлено из номенклатуры
    # ================================================================
    (162, 'INV_FOOD_SWEET', 'Кондитерские изделия',    64, 2, 'consumable','inventory','quantitative', 10, 627, 628, 'INVENTORY/INV_FOOD/INV_FOOD_SWEET', 'Конфеты, шоколад, вафли, карамель', 1),
    (163, 'INV_FOOD_WATER', 'Вода и напитки',           64, 2, 'consumable','inventory','batch',        20, 629, 630, 'INVENTORY/INV_FOOD/INV_FOOD_WATER', 'Питьевая вода, соки',               1),
    (164, 'INV_FOOD_OTHER', 'Прочие продукты',          64, 2, 'consumable','inventory','quantitative', 30, 631, 634, 'INVENTORY/INV_FOOD/INV_FOOD_OTHER', 'Прочие продукты питания',           1),

    # ================================================================
    # Уровень 2: МЕДИЦИНА (INV_MEDICAL) — выявлено из номенклатуры
    # ================================================================
    (165, 'INV_MED_FIRSTAID', 'Аптечки и наборы',       65, 2, 'consumable','inventory','individual',   10, 637, 638, 'INVENTORY/INV_MEDICAL/INV_MED_FIRSTAID', 'Аптечки первой помощи',         1),
    (166, 'INV_MED_DRUGS',    'Медикаменты',             65, 2, 'consumable','inventory','batch',        20, 639, 640, 'INVENTORY/INV_MEDICAL/INV_MED_DRUGS',    'Лекарственные препараты',       1),
    (167, 'INV_MED_BANDAGE',  'Перевязочные материалы',  65, 2, 'consumable','inventory','quantitative', 30, 641, 644, 'INVENTORY/INV_MEDICAL/INV_MED_BANDAGE',  'Бинты, пластыри, жгуты',        1),

    # ================================================================
    # Уровень 2: ПРОЧИЕ МЦ (INV_OTHER) — для неклассифицируемых позиций
    # ================================================================
    (168, 'INV_OTHER_AWARDS', 'Награды и документы',    66, 2, 'consumable','inventory','quantitative', 10, 647, 648, 'INVENTORY/INV_OTHER/INV_OTHER_AWARDS', 'Грамоты, дипломы, награды', 1),
    (169, 'INV_OTHER_MISC',   'Разное',                  66, 2, 'material',  'inventory','quantitative', 90, 649, 654, 'INVENTORY/INV_OTHER/INV_OTHER_MISC',   'Прочие позиции',            1),

    # ================================================================
    # Уровень 1: УСЛУГИ (SERVICES)
    # ================================================================
    (170, 'SRV_REPAIR',  'Ремонт и ТО',                3, 1, 'consumable','service','batch', 310, 662, 663, 'SERVICES/SRV_REPAIR',  'Ремонтные работы',              1),
    (171, 'SRV_INSTALL', 'Монтаж и ПНР',               3, 1, 'consumable','service','batch', 320, 664, 665, 'SERVICES/SRV_INSTALL', 'Монтажные работы',              1),
    (172, 'SRV_TRANSP',  'Транспортные услуги',         3, 1, 'consumable','service','batch', 330, 666, 667, 'SERVICES/SRV_TRANSP',  'Перевозки',                     1),
    (173, 'SRV_CONSULT', 'Консультационные услуги',     3, 1, 'consumable','service','batch', 340, 668, 669, 'SERVICES/SRV_CONSULT', 'Консалтинг',                    1),
    (174, 'SRV_RENT',    'Аренда и лизинг',             3, 1, 'consumable','service','batch', 350, 670, 671, 'SERVICES/SRV_RENT',    'Арендные услуги',               1),
    (175, 'SRV_IT',      'IT-услуги и лицензии',        3, 1, 'consumable','service','batch', 360, 672, 679, 'SERVICES/SRV_IT',      'IT-услуги, ПО',                 1),

    # ================================================================
    # Уровень 1: НМА (INTANGIBLE)
    # ================================================================
    (176, 'INT_SW',    'Программное обеспечение', 4, 1, 'asset','intangible','individual', 410, 682, 685, 'INTANGIBLE/INT_SW',    'ПО и приложения',                     1),
    (177, 'INT_LIC',   'Лицензии и права',        4, 1, 'asset','intangible','individual', 420, 686, 689, 'INTANGIBLE/INT_LIC',   'Лицензии, патенты',                   1),
    (178, 'INT_OTHER', 'Прочие НМА',             4, 1, 'asset','intangible','individual', 430, 690, 699, 'INTANGIBLE/INT_OTHER', 'Прочие нематериальные активы',        1),
]


# ------------------------------------------------------------------
# Вспомогательные функции
# ------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def count_categories(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM categories").fetchone()
    return row[0] if row else 0


def _ensure_path_column(conn: sqlite3.Connection):
    """Добавляет колонку path если её нет (для старых БД)."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(categories)").fetchall()]
    if 'path' not in cols:
        conn.execute("ALTER TABLE categories ADD COLUMN path TEXT")
        conn.commit()
        logger.info("✅ Добавлена колонка path в categories")


def seed(conn: sqlite3.Connection, force: bool = False) -> int:
    """
    Вставляет категории в БД.

    Args:
        conn:  соединение с SQLite
        force: если True — сначала очищает таблицу categories

    Returns:
        Количество вставленных/обновлённых строк.
    """
    _ensure_path_column(conn)

    if force:
        logger.warning("⚠️  --force: очищаем таблицу categories (все дочерние записи останутся)")
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DELETE FROM categories")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='categories'")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()
        logger.info("🗑️  Таблица categories очищена")

    # Получаем существующие коды, чтобы не вставлять дубли
    existing_codes = {
        r[0] for r in conn.execute("SELECT code FROM categories").fetchall()
    }
    # Строим карту code→id для resolving parent_id по коду (нужен для новых записей)
    code_to_id = {
        r[0]: r[1]
        for r in conn.execute("SELECT code, id FROM categories").fetchall()
    }
    # Карта seed_id→code из нашего эталонного списка (для разрешения parent_id)
    seed_id_to_code = {r[0]: r[1] for r in CATEGORIES}

    sql_with_id = """
        INSERT OR IGNORE INTO categories
            (id, code, name_ru, parent_id, level, type, accounting_type,
             account_method, sort_order, lft, rgt, path, description, is_active,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """
    sql_no_id = """
        INSERT INTO categories
            (code, name_ru, parent_id, level, type, accounting_type,
             account_method, sort_order, lft, rgt, path, description, is_active,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """

    inserted = 0
    for row in CATEGORIES:
        seed_id, code, name_ru, seed_parent_id, level, typ, acc_type, acc_method, \
            sort_order, lft, rgt, path, description, is_active = row

        if code in existing_codes:
            continue  # уже есть — пропускаем

        # Resolving parent_id: нашли реальный id родителя по его коду
        real_parent_id = None
        if seed_parent_id is not None:
            parent_code = seed_id_to_code.get(seed_parent_id)
            real_parent_id = code_to_id.get(parent_code) if parent_code else None

        # Пробуем вставить с исходным id; если коллизия по PK — без id
        try:
            cur = conn.execute(sql_with_id, (
                seed_id, code, name_ru, real_parent_id, level, typ, acc_type,
                acc_method, sort_order, lft, rgt, path, description, is_active
            ))
            if cur.rowcount:
                new_id = seed_id
            else:
                # PK занят другой категорией — вставляем без id
                cur2 = conn.execute(sql_no_id, (
                    code, name_ru, real_parent_id, level, typ, acc_type,
                    acc_method, sort_order, lft, rgt, path, description, is_active
                ))
                new_id = cur2.lastrowid
                cur2.close()
        except sqlite3.IntegrityError:
            cur2 = conn.execute(sql_no_id, (
                code, name_ru, real_parent_id, level, typ, acc_type,
                acc_method, sort_order, lft, rgt, path, description, is_active
            ))
            new_id = cur2.lastrowid
            cur2.close()

        code_to_id[code] = new_id
        existing_codes.add(code)
        inserted += 1

    conn.commit()
    logger.info(f"✅ Вставлено: {inserted} / {len(CATEGORIES)} категорий (пропущено существующих)")
    return inserted


def check(conn: sqlite3.Connection):
    """Выводит статистику по категориям в БД."""
    total = count_categories(conn)
    logger.info(f"📊 Категорий в БД: {total}")
    rows = conn.execute("""
        SELECT level, COUNT(*) as cnt
        FROM categories
        GROUP BY level ORDER BY level
    """).fetchall()
    for r in rows:
        logger.info(f"   Уровень {r['level']}: {r['cnt']} шт.")

    # Проверяем, все ли seed-коды присутствуют
    seed_codes = {r[1] for r in CATEGORIES}
    existing = {r[0] for r in conn.execute("SELECT code FROM categories").fetchall()}
    missing = seed_codes - existing
    if missing:
        logger.warning(f"⚠️  Отсутствуют в БД ({len(missing)}): {', '.join(sorted(missing))}")
    else:
        logger.info("✅ Все seed-категории присутствуют в БД")


# ------------------------------------------------------------------
# Точка входа
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Заполнение справочника категорий inventory_bot_V12'
    )
    parser.add_argument('--force',  action='store_true', help='Очистить и заполнить заново')
    parser.add_argument('--check',  action='store_true', help='Только проверить, не менять')
    parser.add_argument('--db',     default=DB_PATH,     help=f'Путь к БД (default: {DB_PATH})')
    args = parser.parse_args()

    if not os.path.exists(args.db):
        logger.error(f"❌ База данных не найдена: {args.db}")
        logger.error("   Сначала запустите приложение (app.py) чтобы создать БД.")
        sys.exit(1)

    conn = get_connection(args.db)

    try:
        if args.check:
            check(conn)
            return

        total_before = count_categories(conn)
        logger.info(f"📦 Категорий до: {total_before}")

        if total_before > 0 and not args.force:
            logger.info("ℹ️  Категории уже есть. Добавляем только отсутствующие (INSERT OR IGNORE).")
            logger.info("   Для полной перезаписи используйте: python seed_categories.py --force")

        inserted = seed(conn, force=args.force)

        total_after = count_categories(conn)
        logger.info(f"📦 Категорий после: {total_after} (добавлено: {total_after - total_before})")
        check(conn)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
