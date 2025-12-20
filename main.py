import customtkinter as ctk
from tkinter import messagebox
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from collections import defaultdict
import json
import math
import numbers
from functools import lru_cache
import threading


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
        self.all_columns = []
        self.display_columns = []  # Колонки для отображения (только те, что есть в данных)
        self.column_types = {}
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

        # Кэширование данных для быстрой загрузки
        self.data_cache = {}
        self.current_query = None
        self.loading_in_progress = False

        # Для управления шириной колонок
        self.column_widths = {}
        self.min_column_width = 120
        self.max_column_width = 400
        self.default_column_width = 200

        # Оптимизация: заранее загружаем схему
        self.detect_schema()

        self.setup_ui()

    def setup_ui(self):
        main_container = ctk.CTkFrame(self.root, fg_color="transparent")
        main_container.pack(fill="both", expand=True, padx=10, pady=10)

        # Удалена верхняя панель с окном и кнопкой статистики

        self.create_filters_panel(main_container)

        # Создаем контейнер для таблицы и панели агрегации
        table_agg_container = ctk.CTkFrame(main_container, fg_color="transparent")
        table_agg_container.pack(side="left", fill="both", expand=True)

        # Создаем общую подложку для таблицы, поиска и пагинации
        self.table_main_container = ctk.CTkFrame(
            table_agg_container,
            corner_radius=12,
            fg_color=("#f0f0f0", "#2a2a2a"),
            border_width=1,
            border_color=("#d0d0d0", "#404040")
        )
        self.table_main_container.pack(fill="both", expand=True, padx=5, pady=5)

        # Создаем контейнер для таблицы и пагинации
        table_pagination_container = ctk.CTkFrame(self.table_main_container, fg_color="transparent")
        table_pagination_container.pack(fill="both", expand=True, padx=10, pady=10)

        # Создаем панель поиска над таблицей
        self.create_search_panel(table_pagination_container)

        # Создаем панель таблицы С ГОРИЗОНТАЛЬНЫМ СКРОЛЛОМ
        self.create_table_panel(table_pagination_container)

        # Создаем панель пагинации под таблицей
        self.create_pagination_panel(table_pagination_container)

        # Создаем панель агрегации с подложкой
        self.create_aggregation_panel(table_agg_container)

        # При запуске сразу показываем все фильтры по всем столбцам
        self.root.after(100, self.create_all_filters)
        self.load_data()

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
            ("#f5f5f5", "#2a2d2e"),  # Светлый/Темный
            ("#f0f8ff", "#2d2a3e"),  # Светлый/Темный фиолетовый
            ("#f8f0ff", "#2a3e2d"),  # Светлый/Темный зеленый
            ("#fff8f0", "#3e2d2a"),  # Светлый/Темный коричневый
            ("#f0fff8", "#2d2d3e"),  # Светлый/Темный синий
        ]
        bg_color = bg_colors[index % len(bg_colors)]

        # Создаем фрейм для условия фильтрации с индивидуальной подложкой
        condition_frame = ctk.CTkFrame(
            self.filters_scroll,
            corner_radius=10,
            fg_color=bg_color,
            border_width=1,
            border_color=("#d0d0d0", "#3a3a3a")
        )
        condition_frame.pack(fill="x", padx=5, pady=5, ipadx=5, ipady=5)

        # Заголовок с номером условия
        header_frame = ctk.CTkFrame(condition_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(5, 10), padx=10)

        ctk.CTkLabel(header_frame, text=f"Фильтр #{filter_id + 1}: {col_name}",
                     font=ctk.CTkFont(weight="bold", size=14)).pack(side="left")

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
            'is_preset': True  # Флаг, что это предустановленный фильтр
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

    def apply_filter_condition(self, filter_id):
        """Применяет одно условие фильтрации"""
        if hasattr(self, '_filter_timer'):
            self.root.after_cancel(self._filter_timer)

        self._filter_timer = self.root.after(500, self.load_data)

    def build_query(self):
        """Строит MongoDB запрос из условий фильтрации"""
        if not self.filter_conditions:
            return {}

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

        # Если нет условий, возвращаем пустой запрос
        if not filter_parts:
            return {}

        # Собираем итоговый запрос - ВСЕ фильтры объединяются через И
        final_query = filter_parts[0]  # Начинаем с первого условия

        for i in range(1, len(filter_parts)):
            next_condition = filter_parts[i]
            # Для И объединяем с $and
            if "$and" not in final_query:
                final_query = {"$and": [final_query, next_condition]}
            else:
                final_query["$and"].append(next_condition)

        # Глобальный поиск
        search_value = self.search_entry.get().strip()
        if search_value:
            # Используем более сложный поиск, как в фильтрах
            search_query = self.build_search_conditions(search_value)
            if search_query:
                if final_query:
                    final_query = {"$and": [final_query, search_query]}
                else:
                    final_query = search_query

        return final_query

    def build_search_conditions(self, search_value):
        """Строит условия поиска по всем полям с разными операторами"""
        if not search_value:
            return None

        try:
            # Разделяем на возможные условия
            conditions = []

            # Проверяем, содержит ли поисковый запрос операторы
            operators = ["равно", "не равно", "больше", "больше или равно",
                         "меньше", "меньше или равно", "в списке", "не в списке"]

            operator_found = False
            for operator in operators:
                if f" {operator} " in search_value:
                    operator_found = True
                    break

            if operator_found:
                # Обрабатываем как сложное условие
                parts = search_value.split()
                if len(parts) >= 3:
                    col_name = parts[0]
                    operator = parts[1]
                    value = " ".join(parts[2:])

                    # Убираем кавычки если есть
                    if (value.startswith('"') and value.endswith('"')) or \
                            (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]

                    condition = self.build_single_condition(col_name, operator, value)
                    if condition:
                        return condition
            else:
                # Простой поиск по всем полям
                or_conditions = []
                for col in self.all_columns:
                    try:
                        # Пытаемся преобразовать в число для числовых сравнений
                        if '.' in search_value:
                            num_value = float(search_value)
                            or_conditions.extend([
                                {col: {"$eq": num_value}},
                                {col: {"$gte": num_value - (num_value * 0.1)}},
                                {col: {"$lte": num_value + (num_value * 0.1)}}
                            ])
                        else:
                            try:
                                num_value = int(search_value)
                                or_conditions.extend([
                                    {col: {"$eq": num_value}},
                                    {col: {"$gte": num_value - 5}},
                                    {col: {"$lte": num_value + 5}}
                                ])
                            except ValueError:
                                # Строковый поиск
                                or_conditions.append({col: {"$regex": search_value, "$options": "i"}})
                    except ValueError:
                        # Строковый поиск
                        or_conditions.append({col: {"$regex": search_value, "$options": "i"}})

                if or_conditions:
                    return {"$or": or_conditions}

        except Exception as e:
            print(f"Ошибка построения условий поиска: {e}")

        # По умолчанию возвращаем простой regex поиск
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
                    # Для НЕ инвертируем условие
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
                    # Для nan и пустых значений используем $or с проверкой на null и nan
                    if operator == "равно":
                        return {"$or": [
                            {col: None},
                            {col: {"$type": "null"}},
                            {col: float('nan')}
                        ]}
                    elif operator == "не равно":
                        return {"$and": [
                            {col: {"$ne": None}},
                            {col: {"$not": {"$type": "null"}}},
                            {col: {"$ne": float('nan')}}
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
                            return {"$and": [
                                {col: {"$ne": None}},
                                {col: {"$not": {"$type": "null"}}},
                                {col: {"$ne": float('nan')}}
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
            placeholder_text="Введите поисковый запрос"
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
        # Основной контейнер таблицы
        self.table_container = ctk.CTkFrame(parent, fg_color="transparent")
        self.table_container.pack(fill="both", expand=True, pady=(0, 10))

        # Создаем CTkScrollableFrame для таблицы с оптимизацией
        self.table_scrollable = ctk.CTkScrollableFrame(
            self.table_container,
            fg_color="transparent",
            border_width=1,
            border_color=("#c0c0c0", "#505050"),
            scrollbar_button_color=("#c0c0c0", "#404040"),
            scrollbar_button_hover_color=("#a0a0a0", "#505050")
        )
        self.table_scrollable.pack(fill="both", expand=True)

        # Контейнер для таблицы (будет заполняться динамически)
        self.table_content_frame = ctk.CTkFrame(self.table_scrollable, fg_color="transparent")
        self.table_content_frame.pack(fill="both", expand=True)

    def create_table_headers(self, columns):
        """Создает заголовки таблицы с горизонтальным скроллом"""
        # Очищаем только если необходимо
        if hasattr(self, 'header_frame') and self.header_frame.winfo_exists():
            for widget in self.header_frame.winfo_children():
                widget.destroy()
        else:
            self.header_frame = ctk.CTkFrame(self.table_content_frame, fg_color="#3a3a3a", height=40)
            self.header_frame.pack(fill="x", pady=(0, 1))

        if not columns:
            return

        col_width = self.default_column_width

        # Настраиваем колонки в гриде - НЕ растягиваем их
        for i in range(len(columns)):
            self.header_frame.grid_columnconfigure(i, weight=0, minsize=col_width)

        for i, col in enumerate(columns):
            # Сохраняем ширину колонки
            self.column_widths[col] = col_width

            # Создаем кнопку-заголовок для сортировки
            header_btn = ctk.CTkButton(
                self.header_frame,
                text=f"{col}↑↓",
                font=ctk.CTkFont(size=12, weight="bold"),
                fg_color="#3a3a3a",
                hover_color="#4a4a4a",
                height=40,
                width=col_width,
                anchor="w",
                command=lambda c=col: self.on_header_click(c)
            )
            header_btn.grid(row=0, column=i, sticky="nsew", padx=(1, 0))

    def create_table_rows(self, data):
        """Создает строки таблицы с данными с горизонтальным скроллом"""
        # Используем быстрый метод очистки
        if hasattr(self, 'data_frame') and self.data_frame.winfo_exists():
            for widget in self.data_frame.winfo_children():
                widget.destroy()
        else:
            self.data_frame = ctk.CTkFrame(self.table_content_frame, fg_color="transparent")
            self.data_frame.pack(fill="both", expand=True)

        if not data:
            # Создаем сообщение "Нет данных"
            no_data_label = ctk.CTkLabel(
                self.data_frame,
                text="Нет данных для отображения",
                font=ctk.CTkFont(size=16, weight="bold"),
                fg_color="transparent"
            )
            no_data_label.pack(expand=True, pady=50)
            return

        # Определяем реальные колонки из данных
        if data:
            first_record = data[0]
            self.display_columns = list(first_record.keys())
        else:
            self.display_columns = []

        col_count = len(self.display_columns)
        col_width = self.default_column_width

        # Настраиваем колонки в гриде - НЕ растягиваем их
        for i in range(col_count):
            self.data_frame.grid_columnconfigure(i, weight=0, minsize=col_width)

        # Создаем строки с данными
        for row_idx, row_data in enumerate(data):
            # Чередование цветов строк
            bg_color = "#2b2b2b" if row_idx % 2 == 0 else "#3a3a3a"

            row_frame = ctk.CTkFrame(self.data_frame, fg_color=bg_color, height=35)
            row_frame.grid(row=row_idx, column=0, sticky="ew", pady=(0, 1))

            # Настраиваем колонки в строке
            for col_idx in range(col_count):
                row_frame.grid_columnconfigure(col_idx, weight=0, minsize=col_width)

            for col_idx, col in enumerate(self.display_columns):
                value = row_data.get(col, "")

                # Форматируем значение
                formatted_value = self.fast_format_value(value)

                # Создаем ячейку с полосой прокрутки по необходимости
                cell = ctk.CTkLabel(
                    row_frame,
                    text=formatted_value,
                    font=ctk.CTkFont(size=11),
                    fg_color=bg_color,
                    height=35,
                    width=col_width,
                    anchor="w",
                    justify="left",
                    wraplength=col_width - 10  # Перенос текста
                )
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=(1, 0))

        # Обновляем ширину контейнера для горизонтального скролла
        self.update_table_width()

    def update_table_width(self):
        """Обновляет ширину контейнера таблицы для включения горизонтального скролла"""
        if not self.display_columns:
            return

        # Рассчитываем общую ширину всех колонок
        total_width = len(self.display_columns) * self.default_column_width

        # Устанавливаем минимальную ширину для контейнера
        min_width = max(800, total_width)  # Минимум 800px или общая ширина колонок

        # Настраиваем ширину контейнеров
        self.table_content_frame.configure(width=min_width)
        self.header_frame.configure(width=min_width)
        self.data_frame.configure(width=min_width)

        # Обновляем отображение
        self.table_content_frame.update_idletasks()

    def fast_format_value(self, value):
        """Быстрое форматирование значения (оптимизированная версия)"""
        if value is None:
            return "[ПУСТО]"

        if isinstance(value, float):
            if math.isnan(value):
                return "[ПУСТО]"
            # Форматируем только если нужно
            if abs(value) > 1000 or (0 < abs(value) < 0.01):
                return f"{value:.2f}"
            return str(value)

        if isinstance(value, (int, numbers.Integral)):
            return str(value)

        if isinstance(value, list):
            return f"[{len(value)}]"

        if isinstance(value, dict):
            return "{...}"

        return str(value)

    def on_header_click(self, column):
        """Обработка клика на заголовок таблицы с защитой от двойного клика"""
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
        """Создает панель агрегации с подложкой"""
        # Контейнер для агрегации с подложкой
        agg_main_container = ctk.CTkFrame(
            parent,
            corner_radius=12,
            fg_color=("#f0f0f0", "#2a2a2a"),
            border_width=1,
            border_color=("#d0d0d0", "#404040")
        )
        agg_main_container.pack(side="bottom", fill="x", padx=5, pady=5)

        agg_header = ctk.CTkFrame(agg_main_container, fg_color="transparent")
        agg_header.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(agg_header, text="📊 Агрегация данных",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(side="left")

        # Главный контейнер для элементов управления агрегацией
        agg_main_controls = ctk.CTkFrame(agg_main_container, fg_color="transparent")
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

    def detect_schema(self):
        """Оптимизированное определение схемы"""
        try:
            sample = self.collection.find_one()
            if sample:
                self.all_columns = [col for col in sample.keys() if col != '_id']

                # Оптимизация: ограничиваем количество записей для анализа
                cursor = self.collection.find({}, {'_id': 0}).limit(500)
                records = list(cursor)

                if records:
                    df_sample = pd.DataFrame(records)
                    for col in self.all_columns:
                        if col in df_sample.columns:
                            dtype = str(df_sample[col].dtype)
                            self.column_types[col] = dtype

                            if df_sample[col].nunique() < 50:  # Уменьшили порог
                                unique_vals = df_sample[col].dropna().unique().tolist()
                                unique_vals_str = [str(val) for val in unique_vals]
                                self.unique_values_cache[col] = sorted(unique_vals_str)[:30]  # Уменьшили количество

                # Обновляем комбобоксы
                if self.all_columns:
                    self.group_by_combo.configure(values=self.all_columns)
                    self.agg_col_combo.configure(values=self.all_columns)

        except Exception as e:
            print(f"Ошибка определения схемы: {e}")

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

        # Создаем фильтры для всех столбцов (но ограничиваем количество для производительности)
        max_filters = min(20, len(self.all_columns))  # Максимум 20 фильтров
        for i in range(max_filters):
            col = self.all_columns[i]
            self.create_filter_for_column(col, i)

        # Обновляем данные после создания всех фильтров
        self.load_data()

    def apply_aggregation(self):
        group_by = self.group_by_var.get()
        agg_func = self.agg_func_var.get()
        agg_col = self.agg_col_combo.get()

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
                group_stage["result"] = {mongo_func: f"${agg_col}"}
            else:
                if mongo_func not in ["$count"]:
                    messagebox.showwarning("Предупреждение",
                                           "Выберите колонку для агрегации")
                    return

            pipeline.append({"$group": group_stage})

            # Сортировка
            sort_direction = self.sort_direction if self.sort_column else 1
            if self.sort_column:
                sort_field = self.sort_column
                if self.sort_column != group_by:
                    sort_field = "result"
                pipeline.append({"$sort": {sort_field: sort_direction}})
            else:
                pipeline.append({"$sort": {"_id": 1}})

            # Ограничиваем количество результатов для быстродействия
            pipeline.append({"$limit": 1000})

            # Выполняем агрегацию в отдельном потоке
            def run_aggregation():
                try:
                    result = list(self.collection.aggregate(pipeline, allowDiskUse=True))
                    self.root.after(0, lambda: self.display_aggregation_results(result, group_by, agg_func, agg_col))
                except Exception as agg_error:
                    self.root.after(0, lambda: messagebox.showwarning("Предупреждение",
                                                                      f"Ошибка агрегации: {str(agg_error)}"))

            threading.Thread(target=run_aggregation, daemon=True).start()

            self.aggregation_mode = True
            self.group_by_column = group_by
            self.aggregation_function = agg_func
            self.aggregation_column = agg_col

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка агрегации: {str(e)}")

    def display_aggregation_results(self, results, group_by, agg_func, agg_col):
        """Отображение результатов агрегации"""
        if not results:
            messagebox.showinfo("Информация", "Нет данных для отображения")
            return

        # Создаем данные для отображения в таблице
        table_data = []

        for record in results:
            row_data = {}
            row_data[group_by] = record.get("_id", "N/A")

            if agg_func == "количество" or "count" in record:
                row_data["Количество"] = record.get("count", 0)
            elif agg_col:
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
        """Оптимизированная загрузка данных"""
        if self.loading_in_progress:
            return

        self.loading_in_progress = True

        # Показываем индикатор загрузки
        self.records_count_label.configure(text="Загрузка...")

        # Запускаем в отдельном потоке
        def load_data_thread():
            try:
                query = self.build_query()

                # Проверяем, изменился ли запрос
                query_key = str(query)

                # Если запрос тот же и у нас есть кэшированное количество записей
                if query_key == self.current_query and 'count' in self.data_cache:
                    self.total_records = self.data_cache['count']
                else:
                    # Считаем только если запрос изменился
                    self.total_records = self.collection.count_documents(query)
                    self.current_query = query_key
                    self.data_cache['count'] = self.total_records

                total_all = self.collection.count_documents({})

                # Обновляем UI в основном потоке
                self.root.after(0, lambda: self.records_count_label.configure(
                    text=f"Найдено: {self.total_records:,} из {total_all:,} записей"
                ))

                self.root.after(0, self.load_page_data)
                self.root.after(0, self.update_info)

            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Ошибка", f"Ошибка загрузки данных: {str(e)}"))
            finally:
                self.loading_in_progress = False

        threading.Thread(target=load_data_thread, daemon=True).start()

    def load_page_data(self):
        """Оптимизированная загрузка страницы данных"""
        skip = self.current_page * self.page_size
        query = self.build_query()

        # Ключ для кэширования
        cache_key = f"{str(query)}_{skip}_{self.page_size}_{self.sort_column}_{self.sort_direction}"

        # Проверяем кэш
        if cache_key in self.data_cache:
            data = self.data_cache[cache_key]
            self.display_data(data)
            return

        # Загружаем данные в отдельном потоке
        def load_page_thread():
            try:
                sort_spec = []
                if self.sort_column:
                    sort_spec = [(self.sort_column, self.sort_direction)]

                cursor = self.collection.find(query, {'_id': 0})

                if sort_spec:
                    cursor = cursor.sort(sort_spec)

                cursor = cursor.skip(skip).limit(self.page_size)

                # Быстрое преобразование данных
                data = []
                for record in cursor:
                    row_data = {}
                    for col in self.all_columns:
                        val = record.get(col, '')
                        # Минимальная обработка для скорости
                        if isinstance(val, float) and math.isnan(val):
                            val = None
                        row_data[col] = val
                    data.append(row_data)

                # Сохраняем в кэш
                self.data_cache[cache_key] = data

                # Отображаем в основном потоке
                self.root.after(0, lambda: self.display_data(data))

            except Exception as e:
                self.root.after(0, lambda: print(f"Ошибка загрузки данных: {e}"))

        threading.Thread(target=load_page_thread, daemon=True).start()

    def display_data(self, data):
        """Быстрое отображение данных"""
        if not data:
            # Если данных нет, создаем заголовки для пустой таблицы
            self.create_table_headers([])
            self.create_table_rows([])
            return

        # Определяем колонки для отображения из данных (только те, которые есть в данных)
        if data:
            # Определяем реальные колонки из всех данных
            all_keys = set()
            for record in data:
                all_keys.update(record.keys())

            # Сортируем колонки для консистентности
            self.display_columns = sorted(list(all_keys))
        else:
            self.display_columns = []

        # Создаем заголовки таблицы (только если нужно)
        if not hasattr(self, 'header_frame') or not self.header_frame.winfo_exists():
            self.create_table_headers(self.display_columns)

        # Создаем строки с данными
        self.create_table_rows(data)

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
            # Очищаем кэш при изменении размера страницы
            self.data_cache.clear()
            self.load_data()
        except:
            pass

    def change_page(self, page_num):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        if 0 <= page_num < total_pages:
            self.current_page = page_num
            self.load_page_data()
            self.update_info()

    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.load_page_data()
            self.update_info()

    def next_page(self):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_page_data()
            self.update_info()

    def last_page(self):
        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        self.change_page(total_pages - 1)

    def apply_search(self):
        self.current_page = 0
        # Очищаем кэш при новом поиске
        self.data_cache.clear()
        self.load_data()

    def apply_sort(self, column, direction):
        if self.sort_column == column:
            self.sort_direction *= -1
        else:
            self.sort_column = column
            self.sort_direction = 1

        # Очищаем кэш сортировки
        self.data_cache = {k: v for k, v in self.data_cache.items() if '_' not in k or k.split('_')[-2] != 'sort'}
        self.load_page_data()

    def clear_all_filters(self):
        # Очищаем все условия во всех фильтрах
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
        # Очищаем кэш
        self.data_cache.clear()
        self.load_data()

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = EnhancedNissanGUI()
    app.run()