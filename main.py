import customtkinter as ctk
from tkinter import ttk, messagebox
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from collections import defaultdict
import math
import numbers
import re


class EnhancedNissanGUI:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.root = ctk.CTk()
        self.root.title("Nissan Vehicles Database - Enhanced")
        self.root.geometry("1800x1000")

        self.client = MongoClient('localhost', 27017)
        self.db = self.client['nissan']
        self.collection = self.db['vehicles']

        self.current_page = 0
        self.page_size = 100
        self.total_records = 0
        self.filtered_records = 0
        self.all_columns = []
        self.column_types = {}
        self.column_stats = {}  # Хранит статистику по колонкам для всех данных
        self.filtered_column_stats = {}  # Хранит статистику по колонкам для отфильтрованных данных
        self.unique_values_cache = defaultdict(list)

        self.filters = {}
        self.sort_column = None
        self.sort_direction = 1
        self.aggregation_pipeline = []

        # Для управления агрегацией
        self.aggregation_mode = False
        self.group_by_column = None
        self.aggregation_function = None
        self.aggregation_column = None

        # Для управления динамическими фильтрами
        self.filter_conditions = []  # Список всех условий фильтрации
        self.filter_widgets = []  # Список виджетов фильтров

        # Переменные для управления двойным кликом
        self.last_click_time = 0
        self.last_click_column = None

        # Фиксированные ширины для столбцов
        self.column_widths = {}

        # Для хранения ссылок на заголовки фильтров
        self.filter_header_labels = {}

        self.setup_ui()

    def setup_ui(self):
        main_container = ctk.CTkFrame(self.root, fg_color="transparent")
        main_container.pack(fill="both", expand=True, padx=10, pady=10)

        self.create_filters_panel(main_container)

        # Создаем контейнер для таблицы и панели агрегации
        table_agg_container = ctk.CTkFrame(main_container, fg_color="transparent")
        table_agg_container.pack(side="left", fill="both", expand=True)

        # Создаем общую подложку для таблицы, поиска и пагинации
        self.table_main_container = ctk.CTkFrame(
            table_agg_container,
            corner_radius=15,
            fg_color="transparent"
        )
        self.table_main_container.pack(fill="both", expand=True, padx=5, pady=5)

        # Создаем контейнер для таблицы и пагинации
        table_pagination_container = ctk.CTkFrame(self.table_main_container, fg_color="transparent")
        table_pagination_container.pack(fill="both", expand=True, padx=10, pady=10)

        # Создаем панель поиска над таблицей
        self.create_search_panel(table_pagination_container)

        # Создаем панель таблицы
        self.create_table_panel(table_pagination_container)
        self.configure_treeview_style()
        # Создаем панель пагинации под таблицей
        self.create_pagination_panel(table_pagination_container)

        # Создаем панель агрегации под таблицей
        self.create_aggregation_panel(table_agg_container)

        self.load_initial_data()

    def configure_treeview_style(self):
        """Настраивает стиль для Treeview с четкими границами ячеек"""
        style = ttk.Style()
        style.theme_use("clam")

        # Настройки для темной темы
        bg_color = "#2b2b2b"  # Цвет фона ячеек
        fg_color = "white"  # Цвет текста
        heading_bg = "#3a3a3a"  # Цвет заголовков
        border_color = "#555555"  # Цвет границ
        selected_bg = "#4a7aba"  # Цвет выделения

        # Основной стиль Treeview
        style.configure("Treeview",
                        background=bg_color,
                        foreground=fg_color,
                        fieldbackground=bg_color,
                        borderwidth=1,
                        relief="solid",  # Добавляем рельеф для границ
                        font=('TkDefaultFont', 10),
                        rowheight=25)

        # Стиль заголовков
        style.configure("Treeview.Heading",
                        background=heading_bg,
                        foreground=fg_color,
                        relief="raised",  # Выпуклые заголовки
                        borderwidth=2,
                        font=('TkDefaultFont', 10, 'bold'),
                        padding=(5, 5, 5, 22))

        # Настраиваем цвета для выделения
        style.map('Treeview',
                  background=[('selected', selected_bg)],
                  foreground=[('selected', 'white')])

        # Настраиваем цвета для заголовков при наведении
        style.map("Treeview.Heading",
                  background=[('active', '#4a4a4a')],
                  relief=[('pressed', 'sunken')])

        # Переопределяем layout для ячеек с явными границами
        style.layout("Treeview.Item", [
            ('Treeitem.padding', {
                'sticky': 'nswe',
                'children': [
                    ('Treeitem.indicator', {'side': 'left', 'sticky': ''}),
                    ('Treeitem.image', {'side': 'left', 'sticky': ''}),
                    ('Treeitem.text', {'side': 'left', 'sticky': 'we'})
                ]
            })
        ])

        # Настройка скроллбаров
        style.configure("Vertical.TScrollbar",
                        background=heading_bg,
                        troughcolor=bg_color,
                        bordercolor=border_color,
                        arrowcolor=fg_color,
                        borderwidth=1)

        style.configure("Horizontal.TScrollbar",
                        background=heading_bg,
                        troughcolor=bg_color,
                        bordercolor=border_color,
                        arrowcolor=fg_color,
                        borderwidth=1)

    def create_filters_panel(self, parent):
        filters_container = ctk.CTkFrame(parent)
        filters_container.pack(side="left", fill="y", padx=(0, 5), pady=5)

        filter_header = ctk.CTkFrame(filters_container, fg_color="transparent")
        filter_header.pack(fill="x", padx=10, pady=10)

        ctk.CTkLabel(filter_header, text="🔍 Фильтры",
                     font=ctk.CTkFont(size=16, weight="bold")).pack(side="left")

        ctk.CTkButton(filter_header, text="Очистить все",
                      width=80, command=self.clear_all_filters).pack(side="right", padx=5)

        self.records_count_label = ctk.CTkLabel(filters_container,
                                                text="Загрузка...",
                                                font=ctk.CTkFont(weight="bold"))
        self.records_count_label.pack(padx=10, pady=(0, 10))

        self.filters_scroll = ctk.CTkScrollableFrame(
            filters_container,
            width=450,
            corner_radius=8
        )
        self.filters_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def create_filter_for_column(self, col_name, index):
        """Создает стандартный фильтр для конкретного столбца"""
        filter_id = len(self.filter_conditions)

        # Цвета подложки для разных фильтров (циклически)
        bg_colors = [
            ("#2a2d2e", "#2a2d2e"),  # Темные тона для темной темы
            ("#2d2a3e", "#2d2a3e"),
            ("#2a3e2d", "#2a3e2d"),
            ("#3e2d2a", "#3e2d2a"),
            ("#2d2d3e", "#2d2d3e"),
        ]
        bg_color = bg_colors[index % len(bg_colors)]

        # Создаем фрейм для условия фильтрации с индивидуальной подложкой
        condition_frame = ctk.CTkFrame(
            self.filters_scroll,
            corner_radius=10,
            fg_color=bg_color,
            border_width=1,
            border_color=("#3a3a3a", "#3a3a3a")
        )
        condition_frame.pack(fill="x", padx=5, pady=5, ipadx=5, ipady=5)

        # Заголовок с номером условия и статистикой
        header_frame = ctk.CTkFrame(condition_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(5, 10), padx=10)

        # Добавляем статистику по колонке (непустые/всего) - для всех данных
        stats_text = ""
        if col_name in self.column_stats:
            stats = self.column_stats[col_name]
            stats_text = f" ({stats['non_empty']:,}/{stats['total']:,})"

        header_label = ctk.CTkLabel(header_frame,
                                    text=f"Фильтр #{filter_id + 1}: {col_name}{stats_text}",
                                    font=ctk.CTkFont(weight="bold", size=14))
        header_label.pack(side="left")

        # Сохраняем ссылку на заголовок для обновления статистики
        self.filter_header_labels[col_name] = header_label

        # Содержимое условия
        content_frame = ctk.CTkFrame(condition_frame, fg_color="transparent")
        content_frame.pack(fill="x", padx=10, pady=(0, 5))

        # Скрываем выбор колонки - она уже задана
        col_var = ctk.StringVar(value=col_name)

        # Фрейм для строк со значениями, логическими операторами и операторами сравнения
        values_container = ctk.CTkFrame(content_frame, fg_color="transparent")
        values_container.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(values_container, text="Условия:", font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(0, 5))

        # Контейнер для строк значений
        values_rows_frame = ctk.CTkFrame(values_container, fg_color="transparent")
        values_rows_frame.pack(fill="x")

        # Создаем первую строку с одним значением (без логического оператора в начале)
        value_rows = []
        first_value_row = self.create_value_row(values_rows_frame, 0, filter_id, is_first=True)
        value_rows.append(first_value_row)

        # Кнопки управления строками значений - ТОЛЬКО КНОПКА ДОБАВЛЕНИЯ
        controls_frame = ctk.CTkFrame(values_container, fg_color="transparent")
        controls_frame.pack(fill="x", pady=(5, 0))

        btn_frame = ctk.CTkFrame(controls_frame, fg_color="transparent")
        btn_frame.pack(side="left")

        # ТОЛЬКО кнопка добавления
        add_btn = ctk.CTkButton(btn_frame, text="+ Добавить условие", width=140, height=28,
                                command=lambda fid=filter_id: self.add_value_row(fid))
        add_btn.pack(side="left", padx=(0, 5))

        # Сохраняем виджеты
        condition_widgets = {
            'frame': condition_frame,
            'col_var': col_var,
            'value_rows': value_rows,  # Список строк значений
            'values_rows_frame': values_rows_frame,
            'value_count': 1,  # Текущее количество строк значений
            'is_preset': True,  # Флаг, что это предустановленный фильтр
            'header_label': header_label,  # Сохраняем ссылку на заголовок
            'col_name': col_name  # Сохраняем имя колонки
        }

        self.filter_conditions.append({
            'id': filter_id,
            'widgets': condition_widgets
        })

        self.filter_widgets.append(condition_widgets)

    def create_value_row(self, parent, row_index, filter_id, is_first=False):
        """Создает строку с логическим оператором, оператором сравнения и полем ввода значения"""
        row_frame = ctk.CTkFrame(parent, fg_color="transparent")
        row_frame.pack(fill="x", pady=(0, 5))

        # Логический оператор (не показываем для первой строки)
        logic_var = None
        if not is_first:
            logic_var = ctk.StringVar(value="И")
            logic_combo = ctk.CTkComboBox(row_frame,
                                          values=["И", "ИЛИ", "НЕ"],
                                          variable=logic_var,
                                          width=70,
                                          height=28)
            logic_combo.pack(side="left", padx=(0, 3))
            logic_combo.bind("<<ComboboxSelected>>",
                             lambda e, fid=filter_id, idx=row_index: self.on_value_logic_change(fid, idx))

        # Оператор сравнения для этого значения
        operator_var = ctk.StringVar(value="равно")
        operator_combo = ctk.CTkComboBox(row_frame,
                                         values=["равно", "не равно", "больше", "больше или равно",
                                                 "меньше", "меньше или равно", "в списке", "не в списке"],
                                         variable=operator_var,
                                         width=160,
                                         height=28)

        # Для первой строки без логического оператора
        if is_first:
            operator_combo.pack(side="left", padx=(0, 5))
        else:
            operator_combo.pack(side="left", padx=(0, 5))

        operator_combo.bind("<<ComboboxSelected>>",
                            lambda e, fid=filter_id, idx=row_index: self.on_value_operator_change(fid, idx))

        # Поле для значения
        value_entry = ctk.CTkEntry(row_frame,
                                   placeholder_text="Введите значение",
                                   height=28)
        value_entry.pack(side="left", padx=(0, 5), fill="x", expand=True)
        value_entry.bind("<KeyRelease>", lambda e, fid=filter_id: self.apply_filter_condition(fid))

        # Добавляем кнопку удаления этой строки на ВСЕ условия кроме первого (row_index > 0)
        remove_btn = None
        if not is_first:
            remove_btn = ctk.CTkButton(row_frame, text="✕", width=28, height=28,
                                       fg_color=("#ff6b6b", "#d32f2f"),
                                       hover_color=("#ff5252", "#b71c1c"),
                                       command=lambda fid=filter_id, rid=row_index:
                                       self.remove_specific_value_row(fid, rid))
            remove_btn.pack(side="left")

        return {
            'frame': row_frame,
            'value_entry': value_entry,
            'operator_var': operator_var,
            'logic_var': logic_var,
            'row_index': row_index,
            'remove_btn': remove_btn
        }

    def on_value_logic_change(self, filter_id, row_index):
        """Обработка изменения логического оператора для значения"""
        self.apply_filter_condition(filter_id)

    def on_value_operator_change(self, filter_id, row_index):
        """Обработка изменения оператора сравнения для значения"""
        widgets = self.filter_conditions[filter_id]['widgets']
        if 0 <= row_index < len(widgets['value_rows']):
            row = widgets['value_rows'][row_index]
            operator = row['operator_var'].get()

            # Для операторов "в списке" и "не в списке" показываем подсказку
            if operator in ["в списке", "не в списке"]:
                row['value_entry'].configure(placeholder_text="Через запятую")
            else:
                row['value_entry'].configure(placeholder_text="Введите значение")

        self.apply_filter_condition(filter_id)

    def add_value_row(self, filter_id):
        """Добавляет новую строку с условием"""
        if 0 <= filter_id < len(self.filter_conditions):
            widgets = self.filter_conditions[filter_id]['widgets']
            values_rows_frame = widgets['values_rows_frame']

            new_row_index = len(widgets['value_rows'])
            new_row = self.create_value_row(values_rows_frame, new_row_index, filter_id, is_first=False)

            widgets['value_rows'].append(new_row)
            widgets['value_count'] += 1

            # Обновляем окно для корректного отображения
            self.filters_scroll.update_idletasks()
            self.apply_filter_condition(filter_id)

    def remove_specific_value_row(self, filter_id, row_index):
        """Удаляет конкретную строку с условием по индексу"""
        if 0 <= filter_id < len(self.filter_conditions):
            widgets = self.filter_conditions[filter_id]['widgets']
            if widgets['value_count'] > 1:
                # Проверяем, не является ли это первой строкой
                if row_index == 0:
                    messagebox.showwarning("Предупреждение",
                                           "Нельзя удалить первое условие в фильтре")
                    return

                # Проверяем, существует ли строка с таким индексом
                if row_index < len(widgets['value_rows']):
                    # Получаем строку для удаления
                    row_to_remove = widgets['value_rows'][row_index]

                    # Удаляем фрейм
                    row_to_remove['frame'].destroy()

                    # Удаляем из списка
                    widgets['value_rows'].pop(row_index)
                    widgets['value_count'] -= 1

                    # Обновляем индексы оставшихся строк
                    for i, remaining_row in enumerate(widgets['value_rows']):
                        remaining_row['row_index'] = i

                        # Обновляем команду кнопки удаления для оставшихся строк
                        if remaining_row['remove_btn']:
                            remaining_row['remove_btn'].configure(
                                command=lambda fid=filter_id, rid=i:
                                self.remove_specific_value_row(fid, rid))

                    # Обновляем и применяем фильтр
                    self.apply_filter_condition(filter_id)

                    # Обновляем окно
                    self.filters_scroll.update_idletasks()

    def remove_filter_condition(self, filter_id):
        """Удаляет условие фильтрации"""
        if 0 <= filter_id < len(self.filter_conditions):
            # Не позволяем удалить стандартные фильтры
            widgets = self.filter_conditions[filter_id]['widgets']
            if widgets.get('is_preset', False):
                messagebox.showwarning("Предупреждение", "Нельзя удалить стандартный фильтр")
                return

            # Не позволяем удалить единственный фильтр
            if len(self.filter_conditions) == 1:
                messagebox.showwarning("Предупреждение", "Нельзя удалить единственный фильтр")
                return

            # Удаляем фрейм с виджетами
            self.filter_conditions[filter_id]['widgets']['frame'].destroy()

            # Удаляем из списков
            del self.filter_conditions[filter_id]
            del self.filter_widgets[filter_id]

            # Обновляем ID оставшихся условий
            for i, condition in enumerate(self.filter_conditions):
                condition['id'] = i
                # Обновляем заголовок
                widgets = condition['widgets']
                for child in widgets['frame'].winfo_children():
                    if isinstance(child, ctk.CTkFrame):
                        for grandchild in child.winfo_children():
                            if isinstance(grandchild, ctk.CTkLabel) and "Фильтр" in grandchild.cget("text"):
                                # Для стандартных фильтров показываем название колонки
                                if widgets.get('is_preset', False):
                                    col_name = widgets['col_var'].get()
                                    # Обновляем статистику
                                    stats_text = ""
                                    if col_name in self.column_stats:
                                        stats = self.column_stats[col_name]
                                        stats_text = f" ({stats['non_empty']:,}/{stats['total']:,})"
                                    grandchild.configure(text=f"Фильтр #{i + 1}: {col_name}{stats_text}")
                                else:
                                    grandchild.configure(text=f"Фильтр #{i + 1}")
                                break

            # Обновляем данные
            self.load_data()

    def apply_filter_condition(self, filter_id):
        """Применяет одно условие фильтрации"""
        if hasattr(self, '_filter_timer'):
            self.root.after_cancel(self._filter_timer)

        self._filter_timer = self.root.after(500, self.load_data)

    def build_query(self):
        """Строит MongoDB запрос из условий фильтрации"""
        final_query = {}

        # Сначала применяем условия фильтрации из фильтров-панелей
        if self.filter_conditions:
            filter_parts = []

            for i, condition in enumerate(self.filter_conditions):
                widgets = condition['widgets']

                col = widgets['col_var'].get()

                # Получаем значения, операторы сравнения и логические операторы из всех строк
                value_conditions = []
                logic_operators = []

                for j, row in enumerate(widgets['value_rows']):
                    val = row['value_entry'].get().strip()
                    operator = row['operator_var'].get()

                    if val:
                        value_conditions.append({
                            'value': val,
                            'operator': operator
                        })

                        # Для первой строки нет логического оператора
                        if j > 0:
                            logic = row['logic_var'].get() if row['logic_var'] else "И"
                            logic_operators.append(logic)

                # Пропускаем пустые условия
                if not col or not value_conditions:
                    continue

                # Строим условие с учетом логических операторов между значениями
                condition_dict = self.build_value_conditions(col, value_conditions, logic_operators)

                if condition_dict:
                    filter_parts.append(condition_dict)

            # Если есть условия фильтрации, объединяем их через И
            if filter_parts:
                if len(filter_parts) == 1:
                    final_query = filter_parts[0]
                else:
                    final_query = {"$and": filter_parts}

        # Затем применяем глобальный поиск по всем полям
        search_value = self.search_entry.get().strip()
        if search_value:
            search_query = self.build_search_conditions(search_value)
            if search_query:
                # Если уже есть условия фильтрации, объединяем с поиском через И
                if final_query:
                    final_query = {"$and": [final_query, search_query]}
                else:
                    final_query = search_query

        return final_query

    def build_search_conditions(self, search_value):
        """Строит условия поиска по всем полям"""
        if not search_value:
            return None

        try:
            # Создаем список условий для поиска по все полям
            or_conditions = []

            # Пытаемся определить, является ли поисковое значение числом
            is_numeric = False
            numeric_value = None

            try:
                if '.' in search_value:
                    numeric_value = float(search_value)
                else:
                    numeric_value = int(search_value)
                is_numeric = True
            except ValueError:
                is_numeric = False

            # Создаем условия для каждого поля
            for col in self.all_columns:
                # Для числовых значений пытаемся искать как число
                if is_numeric:
                    or_conditions.append({col: numeric_value})

                # Всегда добавляем строковый поиск (регистронезависимый)
                or_conditions.append({col: {"$regex": search_value, "$options": "i"}})

            # Если есть условия поиска, возвращаем их
            if or_conditions:
                return {"$or": or_conditions}
            else:
                return None

        except Exception as e:
            print(f"Ошибка построения условий поиска: {e}")
            # Возвращаем простой regex поиск по всем полям
            or_conditions = []
            for col in self.all_columns:
                or_conditions.append({col: {"$regex": search_value, "$options": "i"}})

            return {"$or": or_conditions} if or_conditions else None

    def build_value_conditions(self, col, value_conditions, logic_operators):
        """Строит условия для колонки с учетом операторов сравнения и логических операторов между значениями"""
        if not col or not value_conditions:
            return None

        try:
            # Если только одно условие
            if len(value_conditions) == 1:
                vc = value_conditions[0]
                return self.build_single_condition(col, vc['operator'], vc['value'])

            # Если несколько условий, объединяем их с учетом логических операторов
            conditions = []

            for vc in value_conditions:
                condition = self.build_single_condition(col, vc['operator'], vc['value'])
                if condition:
                    conditions.append(condition)

            if not conditions:
                return None

            if len(conditions) == 1:
                return conditions[0]

            # Объединяем условия с учетом логических операторов
            combined_condition = conditions[0]

            for i in range(1, len(conditions)):
                if i - 1 < len(logic_operators):
                    logic = logic_operators[i - 1]
                else:
                    logic = "И"  # По умолчанию

                if logic == "И":
                    combined_condition = {"$and": [combined_condition, conditions[i]]}
                elif logic == "ИЛИ":
                    combined_condition = {"$or": [combined_condition, conditions[i]]}
                elif logic == "НЕ":
                    # Для НЕ используем $not только для одного условия
                    combined_condition = {"$and": [combined_condition, {"$not": conditions[i]}]}

            return combined_condition

        except Exception as e:
            print(f"Ошибка построения условий: {e}")
            return None

    def build_single_condition(self, col, operator, value):
        """Строит одно условие для MongoDB с учетом nan значений как пустых"""
        if not col or not value:
            return None

        try:
            # Определяем MongoDB оператор
            mongo_operator = {
                "равно": "$eq",
                "не равно": "$ne",
                "больше": "$gt",
                "больше или равно": "$gte",
                "меньше": "$lt",
                "меньше или равно": "$lte",
                "в списке": "$in",
                "не в списке": "$nin"
            }.get(operator, "$eq")

            # Для операторов сравнения
            if mongo_operator in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"]:
                # Обработка специальных значений
                if value.lower() == "nan" or value == "" or value == "[ПУСТО]":
                    # Для nan и пустых значений
                    if operator == "равно":
                        return {"$or": [
                            {col: None},
                            {col: {"$type": "null"}},
                            {col: float('nan')}
                        ]}
                    elif operator == "не равно":
                        # Исправляем: используем $nor вместо $and с $not
                        return {"$nor": [
                            {col: None},
                            {col: {"$type": "null"}},
                            {col: float('nan')}
                        ]}
                    else:
                        # Для других операторов с пустыми значениями возвращаем None
                        return None

                # Пытаемся преобразовать в число
                try:
                    if '.' in value:
                        num_value = float(value)
                    else:
                        num_value = int(value)

                    return {col: {mongo_operator: num_value}}
                except ValueError:
                    # Если не число, используем как строку
                    return {col: {mongo_operator: value}}

            # Для операторов списка
            elif mongo_operator in ["$in", "$nin"]:
                # Разделяем значения, если они введены через запятую
                if ',' in value:
                    values_list = [v.strip() for v in value.split(',')]
                else:
                    values_list = [value]

                # Пытаемся преобразовать в числа
                numeric_values = []
                string_values = []

                for val in values_list:
                    # Проверяем на специальные значения
                    if val.lower() == "nan" or val == "" or val == "[ПУСТО]":
                        # Для nan добавляем специальную обработку
                        if mongo_operator == "$in":
                            return {"$or": [
                                {col: None},
                                {col: {"$type": "null"}},
                                {col: float('nan')}
                            ]}
                        else:  # $nin
                            # Исправляем: используем $nor вместо $and с $not
                            return {"$nor": [
                                {col: None},
                                {col: {"$type": "null"}},
                                {col: float('nan')}
                            ]}

                    try:
                        if '.' in val:
                            num_val = float(val)
                        else:
                            num_val = int(val)
                        numeric_values.append(num_val)
                    except ValueError:
                        string_values.append(val)

                # Если есть только числа, используем их
                if numeric_values and not string_values:
                    return {col: {mongo_operator: numeric_values}}
                # Если есть только строки, используем их
                elif string_values and not numeric_values:
                    return {col: {mongo_operator: string_values}}
                # Если есть и то и другое, используем строки
                else:
                    return {col: {mongo_operator: values_list}}

        except Exception as e:
            print(f"Ошибка построения условия: {e}")
            return None

    def create_search_panel(self, parent):
        """Создает панель поиска над таблицей"""
        search_container = ctk.CTkFrame(parent, fg_color="transparent")
        search_container.pack(fill="x", pady=(5, 10))

        # Внутренний фрейм для элементов управления поиском
        search_inner_frame = ctk.CTkFrame(search_container, fg_color="transparent")
        search_inner_frame.pack(fill="x")

        # Используем grid для точного позиционирования
        search_inner_frame.grid_columnconfigure(2, weight=1)  # Поле ввода растягивается

        # Увеличиваем размер лупы
        ctk.CTkLabel(search_inner_frame, text="🔍", font=ctk.CTkFont(size=18)).grid(
            row=0, column=0, padx=(0, 8), sticky="w")

        ctk.CTkLabel(search_inner_frame, text="Поиск по всем полям:",
                     font=ctk.CTkFont(size=12, weight="bold")).grid(
            row=0, column=1, padx=(0, 10), sticky="w")

        # Поле поиска растягивается от лейбла до кнопки
        self.search_entry = ctk.CTkEntry(
            search_inner_frame,
            height=32,
            placeholder_text="Введите текст для поиска по всем полям"
        )
        self.search_entry.grid(row=0, column=2, sticky="ew", padx=(0, 8))
        self.search_entry.bind("<Return>", lambda e: self.apply_search())

        # Кнопки поиска и очистки
        button_frame = ctk.CTkFrame(search_inner_frame, fg_color="transparent")
        button_frame.grid(row=0, column=3, sticky="e")

        ctk.CTkButton(button_frame, text="Искать", width=80, height=32,
                      command=self.apply_search).pack(side="left", padx=(0, 5))

        ctk.CTkButton(button_frame, text="Очистить", width=80, height=32,
                      command=self.clear_search).pack(side="left")

    def clear_search(self):
        """Очищает поле поиска"""
        self.search_entry.delete(0, 'end')
        self.apply_search()

    def create_table_panel(self, parent):
        """Создает панель таблицы с использованием ttk.Treeview для производительности"""
        # Основной контейнер таблицы
        self.table_container = ctk.CTkFrame(parent, fg_color="transparent")
        self.table_container.pack(fill="both", expand=True, pady=(0, 10))

        # Контейнер для Treeview и скроллбаров
        tree_container = ctk.CTkFrame(self.table_container, fg_color="transparent")
        tree_container.pack(fill="both", expand=True)

        # Создаем вертикальную прокрутку
        v_scrollbar = ctk.CTkScrollbar(tree_container, orientation="vertical")
        v_scrollbar.pack(side="right", fill="y")

        # Создаем горизонтальную прокрутку
        h_scrollbar = ctk.CTkScrollbar(tree_container, orientation="horizontal")
        h_scrollbar.pack(side="bottom", fill="x")

        # Создаем Treeview с расширенными параметрами для границ
        self.tree = ttk.Treeview(
            tree_container,
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set,
            height=25,
            selectmode="browse",
            show="tree headings",  # Показываем заголовки
            style="Treeview"  # Используем наш стиль
        )
        self.tree.pack(side="left", fill="both", expand=True)

        # Настраиваем scrollbars
        v_scrollbar.configure(command=self.tree.yview)
        h_scrollbar.configure(command=self.tree.xview)

        # Привязываем события
        self.tree.bind("<Button-1>", self.on_tree_click)

    def create_table_headers(self, columns):
        """Создает заголовки таблицы для Treeview с фиксированной шириной и многострочным текстом"""
        if not columns:
            return

        # Очищаем существующие колонки
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
            self.tree.column(col, width=0)

        # Устанавливаем новые колонки
        self.tree["columns"] = columns
        self.tree.heading("#0", text="", anchor="w")
        self.tree.column("#0", width=0, stretch=False)

        # Рассчитываем общую ширину всех столбцов
        total_width = 0

        for i, col in enumerate(columns):
            # Создаем заголовок с символом сортировки
            sort_symbol = ""
            if self.sort_column == col:
                sort_symbol = " ↑" if self.sort_direction == 1 else " ↓"

            # Добавляем статистику во вторую строку заголовка
            stats_text = ""
            if col in self.filtered_column_stats and not self.aggregation_mode:
                stats = self.filtered_column_stats[col]
                stats_text = f"\n({stats['non_empty']:,}/{stats['total']:,})"
            elif col in self.column_stats:
                stats = self.column_stats[col]
                stats_text = f"\n({stats['non_empty']:,}/{stats['total']:,})"

            # Создаем многострочный текст заголовка
            header_text = f"{col}{sort_symbol}{stats_text}"

            # Настраиваем колонку
            self.tree.heading(col, text=header_text, anchor="center",
                              command=lambda c=col: self.on_header_click(c))

            # Устанавливаем ширину
            if col in self.column_widths:
                col_width = self.column_widths[col]
            else:
                # Определяем ширину на основе содержимого
                col_width = self.calculate_column_width(col, header_text)
                self.column_widths[col] = col_width

            total_width += col_width

            # Устанавливаем ширину с отступами
            self.tree.column(col, width=col_width, minwidth=col_width,
                             anchor="w", stretch=False)

        # Настраиваем растягивание, чтобы столбцы заполняли всю ширину таблицы
        self.configure_column_stretch(columns, total_width)

    def calculate_column_width(self, col_name, header_text):
        """Рассчитывает фиксированную ширину для колонки на основе содержимого заголовка"""
        # Разделяем текст на строки
        lines = header_text.split('\n')

        # Находим максимальную длину строк
        max_line_len = 0
        for line in lines:
            line_len = len(line)
            if line_len > max_line_len:
                max_line_len = line_len

        # Рассчитываем ширину на основе максимальной длины строки
        base_width = max_line_len * 7  # 7 пикселей на символ

        # Минимальная и максимальная ширина
        min_width = 120
        max_width = 400

        # Ограничиваем ширину
        width = max(min_width, min(max_width, base_width + 20))

        return width

    def configure_column_stretch(self, columns, total_width):
        """Настраивает растягивание столбцов для заполнения всей ширины таблицы"""
        # Получаем ширину контейнера таблицы
        container_width = self.table_container.winfo_width()

        if container_width > 1 and total_width > 0:
            # Рассчитываем коэффициент растяжения
            stretch_factor = container_width / total_width

            # Обновляем ширину каждого столбца
            for i, col in enumerate(columns):
                if col in self.column_widths:
                    new_width = int(self.column_widths[col] * stretch_factor)
                    # Ограничиваем минимальную ширину
                    new_width = max(100, new_width)
                    self.tree.column(col, width=new_width)

    def create_table_rows(self, data):
        """Создает строки таблицы с данными в Treeview"""
        # Очищаем предыдущие данные
        for item in self.tree.get_children():
            self.tree.delete(item)

        if not data:
            # Создаем заголовки даже при отсутствии данных
            if not self.aggregation_mode:
                columns = self.all_columns
            else:
                columns = []

            if columns:
                self.create_table_headers(columns)
            return

        # Определяем колонки для отображения
        if self.aggregation_mode:
            columns = list(data[0].keys()) if data else []
        else:
            columns = self.all_columns

        # Если колонки еще не установлены, устанавливаем их
        if not self.tree["columns"] or len(self.tree["columns"]) != len(columns):
            self.tree["columns"] = columns
            self.create_table_headers(columns)

        # Добавляем данные в Treeview
        for row_idx, row_data in enumerate(data):
            values = []
            for col in columns:
                value = row_data.get(col, "")
                formatted_value = self.safe_format_value(value)
                values.append(formatted_value)

            # Добавляем строку в Treeview
            item = self.tree.insert("", "end", iid=str(row_idx), values=values)

            # Альтернативный цвет для четных строк
            if row_idx % 2 == 0:
                self.tree.item(item, tags=('even_row',))
            else:
                self.tree.item(item, tags=('odd_row',))

        # Настраиваем теги для альтернативных цветов строк
        self.tree.tag_configure('even_row', background='#2b2b2b')
        self.tree.tag_configure('odd_row', background='#252525')

    def on_tree_click(self, event):
        """Обработка клика в Treeview"""
        region = self.tree.identify("region", event.x, event.y)
        if region == "heading":
            column = self.tree.identify_column(event.x)
            if column != "#0":
                col_index = int(column.replace("#", "")) - 1
                columns = self.tree["columns"]
                if col_index < len(columns):
                    col_name = columns[col_index]
                    self.on_header_click(col_name)

    def on_header_click(self, column):
        """Обработка клика на заголовок таблицы"""
        current_time = datetime.now().timestamp()

        # Проверяем, не был ли это двойной клик (менее 0.3 секунды)
        if (current_time - self.last_click_time < 0.3 and
                self.last_click_column == column):
            # Пропускаем двойной клик
            self.last_click_time = 0
            self.last_click_column = None
            return

        # Обновляем время и колонку последнего клика
        self.last_click_time = current_time
        self.last_click_column = column

        # Применяем сортировку
        if self.aggregation_mode:
            self.apply_aggregation_sort(column)
        else:
            self.apply_sort(column, -1 if self.sort_direction == 1 else 1)

    def apply_aggregation_sort(self, column):
        """Применяет сортировку в режиме агрегации"""
        if self.sort_column == column:
            self.sort_direction *= -1
        else:
            self.sort_column = column
            self.sort_direction = 1

        # Перезагружаем агрегацию с новой сортировкой
        self.apply_aggregation()

    def create_pagination_panel(self, parent):
        """Создает панель пагинации под таблицей"""
        pagination_frame = ctk.CTkFrame(parent, fg_color="transparent")
        pagination_frame.pack(fill="x", pady=(5, 0))

        # Левая часть с кнопками навигации и информацией о странице
        left_controls = ctk.CTkFrame(pagination_frame, fg_color="transparent")
        left_controls.pack(side="left", fill="x", expand=True)

        # Кнопки навигации (привязаны к левой стороне)
        nav_frame = ctk.CTkFrame(left_controls, fg_color="transparent")
        nav_frame.pack(side="left", padx=(0, 10))

        # Кнопки "Первая" и "Последняя" одинакового размера (90px)
        ctk.CTkButton(nav_frame, text="⏮ Первая", width=90, height=32,
                      command=lambda: self.change_page(0)).pack(side="left", padx=2)

        # Кнопки "Назад" и "Вперед" одинакового размера (80px)
        ctk.CTkButton(nav_frame, text="◀ Назад", width=80, height=32,
                      command=self.prev_page).pack(side="left", padx=2)

        ctk.CTkButton(nav_frame, text="Вперед ▶", width=80, height=32,
                      command=self.next_page).pack(side="left", padx=2)

        ctk.CTkButton(nav_frame, text="Последняя ⏭", width=90, height=32,
                      command=self.last_page).pack(side="left", padx=2)

        # Информация о странице (после кнопки "Последняя")
        self.page_label = ctk.CTkLabel(left_controls, text="Страница 1 из 1", font=ctk.CTkFont(weight="bold"))
        self.page_label.pack(side="left", padx=(10, 20))

        # Правая часть с выбором страницы и количеством строк
        right_controls = ctk.CTkFrame(pagination_frame, fg_color="transparent")
        right_controls.pack(side="right")

        # Выбор страницы через комбобокс (заменяет textbox и выпадающий список)
        page_combo_frame = ctk.CTkFrame(right_controls, fg_color="transparent")
        page_combo_frame.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(page_combo_frame, text="Перейти на:").pack(side="left", padx=(0, 5))

        # Комбобокс с возможностью фильтрации
        self.page_combo_var = ctk.StringVar(value="1")
        self.page_combo = ctk.CTkComboBox(
            page_combo_frame,
            values=["1"],
            variable=self.page_combo_var,
            width=100,
            height=32,
            command=self.go_to_page_from_combo,
            state="normal"  # Позволяет вводить текст для фильтрации
        )
        self.page_combo.pack(side="left")
        # Привязываем Enter для перехода на страницу
        self.page_combo.bind("<Return>", lambda e: self.go_to_specific_page_from_input())

        # Выбор количества строк на странице ПОСЛЕ выбора страницы
        page_size_frame = ctk.CTkFrame(right_controls, fg_color="transparent")
        page_size_frame.pack(side="left")

        ctk.CTkLabel(page_size_frame, text="Строк на странице:").pack(side="left", padx=(0, 5))
        self.page_size_var = ctk.StringVar(value="100")
        page_size_combo = ctk.CTkComboBox(page_size_frame,
                                          values=["50", "100", "200", "500", "1000"],
                                          variable=self.page_size_var,
                                          width=80,
                                          height=32,
                                          command=self.change_page_size)
        page_size_combo.pack(side="left")

    def go_to_page_from_combo(self, choice):
        """Обработчик выбора страницы из комбобокса"""
        try:
            page_num = int(choice) - 1
            self.change_page(page_num)
        except:
            # Если выбор невалидный, игнорируем
            pass

    def go_to_specific_page_from_input(self):
        """Переход на конкретную страницу из ввода в комбобоксе"""
        try:
            page_num = int(self.page_combo_var.get()) - 1
            self.change_page(page_num)
        except:
            messagebox.showwarning("Предупреждение", "Введите корректный номер страницы")

    def create_aggregation_panel(self, parent):
        """Создает панель агрегации под таблицей"""
        agg_container = ctk.CTkFrame(
            parent,
            corner_radius=12,
            fg_color="transparent"
        )
        agg_container.pack(side="bottom", fill="x", padx=5, pady=5)

        agg_header = ctk.CTkFrame(agg_container, fg_color="transparent")
        agg_header.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(agg_header, text="📊 Агрегация данных",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(side="left")

        # Главный контейнер для элементов управления агрегацией (все в одной строке)
        agg_main_controls = ctk.CTkFrame(agg_container, fg_color="transparent")
        agg_main_controls.pack(fill="x", padx=10, pady=(0, 10))

        # Все элементы в одной строке
        controls_row = ctk.CTkFrame(agg_main_controls, fg_color="transparent")
        controls_row.pack(fill="x")

        # Группировка
        group_frame = ctk.CTkFrame(controls_row, fg_color="transparent")
        group_frame.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(group_frame, text="Группировать по:").pack(side="left", padx=(0, 8))
        self.group_by_var = ctk.StringVar(value="")
        self.group_by_combo = ctk.CTkComboBox(group_frame,
                                              values=[],
                                              variable=self.group_by_var,
                                              width=180,
                                              height=32)
        self.group_by_combo.pack(side="left")

        # Функция
        func_frame = ctk.CTkFrame(controls_row, fg_color="transparent")
        func_frame.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(func_frame, text="Функция:").pack(side="left", padx=(0, 8))
        self.agg_func_var = ctk.StringVar(value="")
        agg_func_combo = ctk.CTkComboBox(func_frame,
                                         values=["сумма", "среднее", "минимум", "максимум",
                                                 "первое значение", "последнее значение", "все значения",
                                                 "уникальные значения", "количество", "выборочная дисперсия",
                                                 "генерируемая дисперсия"],
                                         variable=self.agg_func_var,
                                         width=180,
                                         height=32)
        agg_func_combo.pack(side="left")

        # Колонка для агрегации
        col_frame = ctk.CTkFrame(controls_row, fg_color="transparent")
        col_frame.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(col_frame, text="Колонка:").pack(side="left", padx=(0, 8))
        self.agg_col_var = ctk.StringVar(value="")
        self.agg_col_combo = ctk.CTkComboBox(col_frame,
                                             values=[],
                                             variable=self.agg_col_var,
                                             width=180,
                                             height=32)
        self.agg_col_combo.pack(side="left")

        # Кнопки управления агрегацией
        button_frame = ctk.CTkFrame(controls_row, fg_color="transparent")
        button_frame.pack(side="left", padx=(0, 10))

        ctk.CTkButton(button_frame, text="Применить агрегацию", width=160, height=32,
                      command=self.apply_aggregation).pack(side="left", padx=5)
        ctk.CTkButton(button_frame, text="Сбросить агрегацию", width=160, height=32,
                      command=self.reset_aggregation).pack(side="left", padx=5)

    def safe_format_value(self, value):
        """Безопасное форматирование значения с обработкой различных типов данных"""
        try:
            if value is None:
                return "[ПУСТО]"

            # Проверяем на NaN (не число)
            if isinstance(value, float):
                if math.isnan(value):
                    return "[ПУСТО]"
                # Форматируем с 2 знаками после запятой
                return f"{value:.2f}"

            # Проверяем на другие числовые типы
            if isinstance(value, (int, numbers.Integral)):
                return str(value)

            # Для списков показываем количество элементов
            if isinstance(value, list):
                return f"[{len(value)} значений]"

            # Для словарей преобразуем в строку
            if isinstance(value, dict):
                return "{...}"

            # Для остальных типов просто преобразуем в строку
            return str(value)

        except Exception as e:
            print(f"Ошибка форматирования значения {value}: {e}")
            return "[ОШИБКА]"

    def load_initial_data(self):
        self.detect_schema()
        # При запуске сразу показываем все фильтры по всем столбцам
        self.root.after(100, self.create_all_filters)
        self.load_data()

    def create_all_filters(self):
        """Создает фильтры для всех столбцов при запуске"""
        if not self.all_columns:
            return

        # Удаляем начальный фильтр если он есть
        if self.filter_conditions:
            for condition in self.filter_conditions:
                condition['widgets']['frame'].destroy()
            self.filter_conditions.clear()
            self.filter_widgets.clear()

        # Создаем фильтры для всех столбцов
        for i, col in enumerate(self.all_columns):
            self.create_filter_for_column(col, i)

        # Обновляем данные после создания всех фильтров
        self.load_data()

    def detect_schema(self):
        try:
            # Получаем общее количество записей из базы данных
            total_records = self.collection.count_documents({})
            print(f"Всего записей в базе: {total_records}")

            if total_records == 0:
                print("База данных пуста")
                return

            # Берем ВСЕ данные для точной статистики
            cursor = self.collection.find({}, {'_id': 0})
            records = list(cursor)

            if not records:
                print("Не удалось получить записи из базы")
                return

            print(f"Получено записей для анализа: {len(records)}")

            df = pd.DataFrame(records)
            self.all_columns = [col for col in df.columns if col != '_id']

            print(f"Найдено колонок: {len(self.all_columns)}")
            print(f"Колонки: {self.all_columns}")

            # Вычисляем точную статистику для каждой колонки
            for col in self.all_columns:
                if col in df.columns:
                    # Получаем тип данных
                    dtype = str(df[col].dtype)
                    self.column_types[col] = dtype

                    # ТОЧНЫЙ расчет непустых значений
                    # Проверяем каждое значение на пустоту (None, NaN, пустая строка)
                    non_empty_count = 0
                    for value in df[col]:
                        if pd.isna(value) or value is None:
                            continue
                        if isinstance(value, float) and math.isnan(value):
                            continue
                        if isinstance(value, str) and value.strip() == "":
                            continue
                        non_empty_count += 1

                    # Сохраняем статистику для всех данных
                    self.column_stats[col] = {
                        'total': total_records,
                        'non_empty': non_empty_count,
                        'empty': total_records - non_empty_count,
                        'fill_rate': (non_empty_count / total_records * 100) if total_records > 0 else 0
                    }

                    print(
                        f"{col}: непустых={non_empty_count:,}, всего={total_records:,}, заполненность={self.column_stats[col]['fill_rate']:.1f}%")

                    # Кэшируем уникальные значения для фильтров
                    if df[col].nunique() < 100:
                        unique_vals = df[col].dropna().unique().tolist()
                        unique_vals_str = [str(val) for val in unique_vals]
                        self.unique_values_cache[col] = sorted(unique_vals_str)[:50]

            # Обновляем комбобоксы
            if self.all_columns:
                self.group_by_combo.configure(values=self.all_columns)
                self.agg_col_combo.configure(values=self.all_columns)

        except Exception as e:
            print(f"Ошибка определения схемы: {e}")
            import traceback
            traceback.print_exc()

    def calculate_filtered_column_stats(self):
        """Рассчитывает статистику по колонкам для отфильтрованных данных"""
        query = self.build_query()

        # Очищаем предыдущую статистику
        self.filtered_column_stats.clear()

        try:
            # Получаем отфильтрованные данные
            cursor = self.collection.find(query, {'_id': 0})
            records = list(cursor)

            if not records:
                # Если нет данных, сбрасываем статистику
                for col in self.all_columns:
                    self.filtered_column_stats[col] = {
                        'total': 0,
                        'non_empty': 0,
                        'empty': 0,
                        'fill_rate': 0
                    }
                return

            df = pd.DataFrame(records)

            # Рассчитываем статистику для каждой колонки
            for col in self.all_columns:
                if col in df.columns:
                    # ТОЧНЫЙ расчет непустых значений для отфильтрованных данных
                    non_empty_count = 0
                    for value in df[col]:
                        if pd.isna(value) or value is None:
                            continue
                        if isinstance(value, float) and math.isnan(value):
                            continue
                        if isinstance(value, str) and value.strip() == "":
                            continue
                        non_empty_count += 1

                    # Сохраняем статистику для отфильтрованных данных
                    self.filtered_column_stats[col] = {
                        'total': len(records),
                        'non_empty': non_empty_count,
                        'empty': len(records) - non_empty_count,
                        'fill_rate': (non_empty_count / len(records) * 100) if len(records) > 0 else 0
                    }
                else:
                    # Если колонка отсутствует в результатах
                    self.filtered_column_stats[col] = {
                        'total': 0,
                        'non_empty': 0,
                        'empty': 0,
                        'fill_rate': 0
                    }

        except Exception as e:
            print(f"Ошибка расчета статистики по отфильтрованным данным: {e}")
            # В случае ошибки сбрасываем статистику
            for col in self.all_columns:
                self.filtered_column_stats[col] = {
                    'total': 0,
                    'non_empty': 0,
                    'empty': 0,
                    'fill_rate': 0
                }

    def update_all_statistics(self):
        """Обновляет всю статистику в интерфейсе"""
        # Обновляем заголовки фильтров
        for condition in self.filter_conditions:
            widgets = condition['widgets']
            col_name = widgets['col_name']

            if col_name:
                # Используем статистику отфильтрованных данных, если она есть
                stats_text = ""
                if col_name in self.filtered_column_stats and not self.aggregation_mode:
                    stats = self.filtered_column_stats[col_name]
                    if stats['total'] > 0:
                        stats_text = f" ({stats['non_empty']:,}/{stats['total']:,})"
                    else:
                        stats = self.column_stats.get(col_name, {'non_empty': 0, 'total': 0})
                        stats_text = f" ({stats['non_empty']:,}/{stats['total']:,})"
                elif col_name in self.column_stats:
                    stats = self.column_stats[col_name]
                    stats_text = f" ({stats['non_empty']:,}/{stats['total']:,})"

                # Обновляем заголовок
                header_text = f"Фильтр #{condition['id'] + 1}: {col_name}{stats_text}"
                widgets['header_label'].configure(text=header_text)

        # Обновляем заголовки таблицы
        self.update_table_headers()

    def update_table_headers(self):
        """Обновляет заголовки таблицы с актуальной статистикой"""
        if not self.tree["columns"]:
            return

        columns = self.tree["columns"]

        for col in columns:
            # Создаем заголовок с символом сортировки
            sort_symbol = ""
            if self.sort_column == col:
                sort_symbol = " ↑" if self.sort_direction == 1 else " ↓"

            # Добавляем статистику во вторую строку заголовка
            # Используем статистику для отфильтрованных данных, если она есть
            stats_text = ""
            if col in self.filtered_column_stats and not self.aggregation_mode:
                stats = self.filtered_column_stats[col]
                if stats['total'] > 0:
                    stats_text = f"\n({stats['non_empty']:,}/{stats['total']:,})"
                else:
                    stats = self.column_stats.get(col, {'non_empty': 0, 'total': 0})
                    stats_text = f"\n({stats['non_empty']:,}/{stats['total']:,})"
            elif col in self.column_stats:
                stats = self.column_stats[col]
                stats_text = f"\n({stats['non_empty']:,}/{stats['total']:,})"

            # Создаем многострочный текст заголовка
            header_text = f"{col}{sort_symbol}{stats_text}"

            # Обновляем заголовок колонки
            self.tree.heading(col, text=header_text)

    def apply_aggregation(self):
        group_by = self.group_by_var.get()
        agg_func = self.agg_func_var.get()
        agg_col = self.agg_col_var.get()

        if not group_by or not agg_func:
            messagebox.showwarning("Предупреждение",
                                   "Выберите колонку для группировки и агрегационную функцию")
            return

        try:
            # Преобразуем человеческое название функции в MongoDB оператор
            mongo_func = {
                "сумма": "$sum",
                "среднее": "$avg",
                "минимум": "$min",
                "максимум": "$max",
                "первое значение": "$first",
                "последнее значение": "$last",
                "все значения": "$push",
                "уникальные значения": "$addToSet",
                "количество": "$count",
                "выборочная дисперсия": "$stdDevPop",
                "генерируемая дисперсия": "$stdDevSamp"
            }.get(agg_func, "$sum")

            # Строим пайплайн агрегации
            pipeline = []

            # Добавляем стадию матча из текущих фильтров
            match_stage = self.build_query()
            if match_stage:
                pipeline.append({"$match": match_stage})

            # Стадия группировки
            group_stage = {"_id": f"${group_by}"}

            if mongo_func == "$count":
                group_stage["count"] = {"$sum": 1}
            elif agg_col:
                # Для большинства функций просто применяем оператор
                if mongo_func in ["$sum", "$avg", "$min", "$max", "$first", "$last", "$push", "$addToSet"]:
                    group_stage["result"] = {mongo_func: f"${agg_col}"}
                elif mongo_func in ["$stdDevPop", "$stdDevSamp"]:
                    # Для стандартного отклонения фильтруем числовые значения
                    group_stage["result"] = {
                        mongo_func: {
                            "$cond": {
                                "if": {"$and": [
                                    {"$ne": [f"${agg_col}", None]},
                                    {"$ne": [{"$type": f"${agg_col}"}, "null"]},
                                    {"$in": [{"$type": f"${agg_col}"}, ["double", "int", "long", "decimal"]]}
                                ]},
                                "then": f"${agg_col}",
                                "else": None
                            }
                        }
                    }
            else:
                # Если колонка не выбрана, но функция требует ее
                if mongo_func not in ["$count"]:
                    messagebox.showwarning("Предупреждение",
                                           "Выберите колонку для агрегации")
                    return

            pipeline.append({"$group": group_stage})

            # Фильтруем группы с пустыми результатами для числовых функций
            if mongo_func in ["$stdDevPop", "$stdDevSamp"]:
                pipeline.append({"$match": {"result": {"$ne": None}}})

            # Сортировка
            sort_direction = self.sort_direction if self.sort_column else 1
            if self.sort_column:
                sort_field = "result" if self.sort_column != group_by else "_id"
                pipeline.append({"$sort": {sort_field: sort_direction}})
            else:
                pipeline.append({"$sort": {"_id": 1}})

            # Выполняем агрегацию
            try:
                result = list(self.collection.aggregate(pipeline, allowDiskUse=True))
            except Exception as agg_error:
                print(f"Ошибка агрегации: {agg_error}")
                messagebox.showwarning("Предупреждение",
                                       f"Ошибка агрегации: {str(agg_error)}\nПопробуйте другие параметры.")
                return

            # Обновляем таблицу с результатами
            self.display_aggregation_results(result, group_by, agg_func, agg_col)

            self.aggregation_mode = True
            self.group_by_column = group_by
            self.aggregation_function = agg_func
            self.aggregation_column = agg_col

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка агрегации: {str(e)}")
            import traceback
            traceback.print_exc()

    def display_aggregation_results(self, results, group_by, agg_func, agg_col):
        """Отображение результатов агрегации"""
        # Создаем данные для отображения в таблице
        table_data = []

        for record in results:
            row_data = {}
            row_data[group_by] = record.get("_id", "N/A")

            if agg_func == "количество" or "count" in record:
                row_data["Количество"] = record.get("count", 0)
            elif agg_col:
                # Красивое отображение функции
                func_display = {
                    "сумма": "Сумма",
                    "среднее": "Среднее",
                    "минимум": "Минимум",
                    "максимум": "Максимум",
                    "первое значение": "Первое",
                    "последнее значение": "Последнее",
                    "все значения": "Все значения",
                    "уникальные значения": "Уникальные",
                    "выборочная дисперсия": "Выб. дисперсия",
                    "генерируемая дисперсия": "Ген. дисперсия"
                }.get(agg_func, agg_func)
                column_name = f"{func_display}({agg_col})"
                row_data[column_name] = record.get("result", 0)

            table_data.append(row_data)

        # Определяем колонки для отображения
        columns = list(table_data[0].keys()) if table_data else []

        # Создаем заголовки
        self.create_table_headers(columns)

        # Создаем строки с данными
        self.create_table_rows(table_data)

        # Обновляем информацию о записях
        self.records_count_label.configure(
            text=f"Агрегировано {len(results):,} групп"
        )

        # Обновляем информацию о странице
        self.page_label.configure(text="Агрегация")
        self.page_combo_var.set("")

    def reset_aggregation(self):
        self.aggregation_mode = False
        self.group_by_column = None
        self.aggregation_function = None
        self.aggregation_column = None

        # Обновляем комбобоксы
        if self.all_columns:
            self.group_by_combo.configure(values=self.all_columns)
            self.agg_col_combo.configure(values=self.all_columns)

        self.group_by_var.set("")
        self.agg_func_var.set("")
        self.agg_col_var.set("")

        # Возвращаемся к обычному отображению
        self.load_data()

    def load_data(self):
        try:
            if self.aggregation_mode:
                # Если в режиме агрегации, не обновляем обычные данные
                return

            query = self.build_query()

            # Исправляем: проверяем запрос перед использованием
            if query:
                self.total_records = self.collection.count_documents(query)
            else:
                self.total_records = self.collection.count_documents({})

            total_all = self.collection.count_documents({})

            # Обновляем метку с количеством записей
            self.records_count_label.configure(
                text=f"Найдено: {self.total_records:,} из {total_all:,} записей"
            )

            # Рассчитываем статистику по отфильтрованным данным
            self.calculate_filtered_column_stats()

            # Обновляем всю статистику в интерфейсе
            self.update_all_statistics()

            self.load_page_data()
            self.update_info()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка загрузки данных: {str(e)}")
            import traceback
            traceback.print_exc()

    def load_page_data(self):
        skip = self.current_page * self.page_size
        query = self.build_query()

        try:
            sort_spec = []
            if self.sort_column:
                sort_spec = [(self.sort_column, self.sort_direction)]

            cursor = self.collection.find(query, {'_id': 0})

            if sort_spec:
                cursor = cursor.sort(sort_spec)

            cursor = cursor.skip(skip).limit(self.page_size)

            # Преобразуем данные в формат для отображения
            data = []
            for record in cursor:
                row_data = {}
                for col in self.all_columns:
                    val = record.get(col, '')
                    # Обработка nan значений
                    if isinstance(val, float) and math.isnan(val):
                        val = None
                    row_data[col] = val
                data.append(row_data)

            # Создаем строки с данными
            self.create_table_rows(data)

        except Exception as e:
            print(f"Ошибка загрузки данных: {e}")
            # Очищаем таблицу в случае ошибки
            for item in self.tree.get_children():
                self.tree.delete(item)

    def update_info(self):
        if self.aggregation_mode:
            return

        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        current_page = min(self.current_page + 1, total_pages)

        self.page_label.configure(text=f"Страница {current_page} из {total_pages}")

        # Обновляем значения в комбобоксе страниц
        page_values = [str(i) for i in range(1, total_pages + 1)]
        self.page_combo.configure(values=page_values)
        self.page_combo_var.set(str(current_page))

    def change_page_size(self, value):
        try:
            self.page_size = int(value)
            self.current_page = 0
            self.load_data()
        except:
            pass

    def change_page(self, page_num):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        if 0 <= page_num < total_pages:
            self.current_page = page_num
            self.load_data()

    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.load_data()

    def next_page(self):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_data()

    def last_page(self):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        self.change_page(total_pages - 1)

    def apply_search(self):
        self.current_page = 0
        self.load_data()

    def apply_sort(self, column, direction):
        if self.sort_column == column:
            self.sort_direction *= -1
        else:
            self.sort_column = column
            self.sort_direction = 1

        self.load_data()

    def clear_all_filters(self):
        # Очищаем все условия во всех фильтрах (оставляем только по одному пустому условию)
        for condition in self.filter_conditions:
            widgets = condition['widgets']

            # Оставляем только первую строку, удаляем остальные
            while len(widgets['value_rows']) > 1:
                last_row = widgets['value_rows'][-1]
                last_frame = last_row['frame']
                last_frame.destroy()
                widgets['value_rows'].pop()
                widgets['value_count'] -= 1

            # Очищаем поле ввода в первой строке
            if widgets['value_rows']:
                widgets['value_rows'][0]['value_entry'].delete(0, 'end')
                widgets['value_rows'][0]['operator_var'].set("равно")
                if widgets['value_rows'][0]['logic_var']:
                    widgets['value_rows'][0]['logic_var'].set("И")

        self.search_entry.delete(0, 'end')
        self.sort_column = None
        self.sort_direction = 1
        self.current_page = 0
        self.load_data()

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = EnhancedNissanGUI()
    app.run()