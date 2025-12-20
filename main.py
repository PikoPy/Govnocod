import customtkinter as ctk
from tkinter import ttk, messagebox, StringVar, BooleanVar
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from collections import defaultdict
import json


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

        self.setup_ui()

    def setup_ui(self):
        main_container = ctk.CTkFrame(self.root)
        main_container.pack(fill="both", expand=True, padx=10, pady=10)

        self.create_top_panel(main_container)
        self.create_filters_panel(main_container)
        self.create_table_panel(main_container)
        self.create_bottom_panel(main_container)
        self.create_aggregation_panel(main_container)

        self.load_initial_data()

    def create_top_panel(self, parent):
        top_frame = ctk.CTkFrame(parent, height=60)
        top_frame.pack(fill="x", padx=0, pady=(0, 5))

        title_label = ctk.CTkLabel(top_frame,
                                   text="🚗 Nissan Vehicles Database",
                                   font=ctk.CTkFont(size=24, weight="bold"))
        title_label.pack(side="left", padx=20)

        button_frame = ctk.CTkFrame(top_frame, fg_color="transparent")
        button_frame.pack(side="right", padx=20)

        ctk.CTkButton(button_frame, text="Статистика",
                      width=100, command=self.show_statistics).pack(side="left", padx=5)

    def create_filters_panel(self, parent):
        filters_container = ctk.CTkFrame(parent)
        filters_container.pack(side="left", fill="y", padx=(0, 5), pady=5)

        filter_header = ctk.CTkFrame(filters_container, fg_color="transparent")
        filter_header.pack(fill="x", padx=10, pady=10)

        ctk.CTkLabel(filter_header, text="🔍 Фильтры",
                     font=ctk.CTkFont(size=16, weight="bold")).pack(side="left")

        # УБИРАЕМ кнопку для добавления нового фильтра - теперь фильтры только стандартные
        # ctk.CTkButton(filter_header, text="+ Добавить фильтр",
        #               width=120, command=self.add_filter_condition).pack(side="right", padx=(5, 0))

        ctk.CTkButton(filter_header, text="Очистить все",
                      width=80, command=self.clear_all_filters).pack(side="right", padx=5)

        self.records_count_label = ctk.CTkLabel(filters_container,
                                                text="Загрузка...",
                                                font=ctk.CTkFont(weight="bold"))
        self.records_count_label.pack(padx=10, pady=(0, 10))

        self.filters_scroll = ctk.CTkScrollableFrame(
            filters_container,
            width=450,  # Увеличена ширина для вмещения всех элементов
            corner_radius=8
        )
        self.filters_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def create_filter_for_column(self, col_name, index):
        """Создает стандартный фильтр для конкретного столбца"""
        filter_id = len(self.filter_conditions)

        # Цвета подложки для разных фильтров (циклически)
        bg_colors = [
            ("#f5f5f5", "#2a2d3e"),  # Светлый серый / Темно-синий
            ("#f0f8ff", "#3a2d4e"),  # AliceBlue / Темно-фиолетовый
            ("#f8f0ff", "#2d4e3a"),  # Лавандовый / Темно-зеленый
            ("#fff8f0", "#4e3a2d"),  # Seashell / Коричневый
            ("#f0fff8", "#3a2d2d"),  # MintCream / Темно-красный
        ]
        bg_color = bg_colors[index % len(bg_colors)]

        # Создаем фрейм для условия фильтрации с индивидуальной подложкой
        condition_frame = ctk.CTkFrame(
            self.filters_scroll,
            corner_radius=10,
            fg_color=bg_color,
            border_width=1,
            border_color=("#d0d0d0", "#404040")
        )
        condition_frame.pack(fill="x", padx=5, pady=5, ipadx=5, ipady=5)

        # Заголовок с номером условия
        header_frame = ctk.CTkFrame(condition_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(5, 10), padx=10)

        ctk.CTkLabel(header_frame, text=f"Фильтр #{filter_id + 1}: {col_name}",
                     font=ctk.CTkFont(weight="bold", size=14)).pack(side="left")

        # НЕ показываем кнопку удаления для стандартных фильтров
        # delete_btn = ctk.CTkButton(header_frame, text="✕ Удалить", width=80,
        #                            fg_color=("#ff6b6b", "#d32f2f"),
        #                            hover_color=("#ff5252", "#b71c1c"),
        #                            command=lambda fid=filter_id: self.remove_filter_condition(fid))
        # delete_btn.pack(side="right", padx=5)

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

        # Кнопки управления строками значений (ПОКАЗЫВАЕМ для стандартных фильтров!)
        controls_frame = ctk.CTkFrame(values_container, fg_color="transparent")
        controls_frame.pack(fill="x", pady=(5, 0))

        btn_frame = ctk.CTkFrame(controls_frame, fg_color="transparent")
        btn_frame.pack(side="left")

        add_btn = ctk.CTkButton(btn_frame, text="+ Добавить условие", width=140, height=28,
                                command=lambda fid=filter_id: self.add_value_row(fid))
        add_btn.pack(side="left", padx=(0, 5))

        remove_btn = ctk.CTkButton(btn_frame, text="- Удалить условие", width=140, height=28,
                                   fg_color=("#ff6b6b", "#d32f2f"),
                                   hover_color=("#ff5252", "#b71c1c"),
                                   command=lambda fid=filter_id: self.remove_value_row(fid))
        remove_btn.pack(side="left")

        # Строка: логический оператор связи с предыдущим фильтром (только если не первый фильтр)
        logic_var = None
        if filter_id > 0:
            row3_frame = ctk.CTkFrame(content_frame, fg_color="transparent")
            row3_frame.pack(fill="x", pady=(10, 0))

            ctk.CTkLabel(row3_frame, text="Связь с предыдущим фильтром:", font=ctk.CTkFont(weight="bold")).pack(
                anchor="w")

            logic_frame = ctk.CTkFrame(row3_frame, fg_color="transparent")
            logic_frame.pack(fill="x", pady=(5, 0))

            logic_var = ctk.StringVar(value="И")
            logic_combo = ctk.CTkComboBox(logic_frame,
                                          values=["И", "ИЛИ", "НЕ", "НИ"],
                                          variable=logic_var,
                                          width=180,
                                          height=32,
                                          command=lambda e, fid=filter_id: self.apply_filter_condition(fid))
            logic_combo.pack(side="left")

        # Сохраняем виджеты
        condition_widgets = {
            'frame': condition_frame,
            'col_var': col_var,
            'logic_var': logic_var,
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
                                          width=80,  # Увеличена ширина
                                          height=28)
            logic_combo.pack(side="left", padx=(0, 5))
            logic_combo.bind("<<ComboboxSelected>>",
                             lambda e, fid=filter_id, idx=row_index: self.on_value_logic_change(fid, idx))

        # Оператор сравнения для этого значения
        operator_var = ctk.StringVar(value="равно")
        operator_combo = ctk.CTkComboBox(row_frame,
                                         values=["равно", "не равно", "больше", "больше или равно",
                                                 "меньше", "меньше или равно", "в списке", "не в списке"],
                                         variable=operator_var,
                                         width=180,  # Увеличена ширина для длинных названий
                                         height=28)

        # Для первой строки без логического оператора - меньше отступ
        if is_first:
            operator_combo.pack(side="left", padx=(0, 10))
        else:
            operator_combo.pack(side="left", padx=(0, 10))

        operator_combo.bind("<<ComboboxSelected>>",
                            lambda e, fid=filter_id, idx=row_index: self.on_value_operator_change(fid, idx))

        # Поле для значения
        value_entry = ctk.CTkEntry(row_frame,
                                   placeholder_text="Введите значение",
                                   height=32)
        value_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
        value_entry.bind("<KeyRelease>", lambda e, fid=filter_id: self.apply_filter_condition(fid))

        # Кнопка удаления этой строки (не показываем для первой строки если она единственная)
        widgets = None
        if filter_id < len(self.filter_conditions):
            widgets = self.filter_conditions[filter_id]['widgets']

        # Показываем кнопку удаления только если это не первая строка или если строк больше одной
        if not is_first or (widgets and widgets['value_count'] > 1):
            remove_btn = ctk.CTkButton(row_frame, text="✕", width=30, height=28,
                                       fg_color=("#ff6b6b", "#d32f2f"),
                                       hover_color=("#ff5252", "#b71c1c"),
                                       command=lambda fid=filter_id, frame=row_frame:
                                       self.remove_specific_value_row(fid, frame))
            remove_btn.pack(side="left")

        return {
            'frame': row_frame,
            'value_entry': value_entry,
            'operator_var': operator_var,
            'logic_var': logic_var,
            'row_index': row_index
        }

    def on_column_change(self, filter_id):
        """Обработка изменения колонки"""
        self.apply_filter_condition(filter_id)

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
                row['value_entry'].configure(placeholder_text="Значения через запятую")
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

            # Обновляем первую строку - теперь должна появиться кнопка удаления
            if widgets['value_count'] > 1 and len(widgets['value_rows']) > 0:
                first_row = widgets['value_rows'][0]
                # Если у первой строки нет кнопки удаления, добавляем ее
                first_frame = first_row['frame']
                # Проверяем, есть ли уже кнопка удаления у первой строки
                has_remove_btn = False
                for child in first_frame.winfo_children():
                    if isinstance(child, ctk.CTkButton) and child.cget("text") == "✕":
                        has_remove_btn = True
                        break

                if not has_remove_btn:
                    # Добавляем кнопку удаления к первой строке
                    remove_btn = ctk.CTkButton(first_frame, text="✕", width=30, height=28,
                                               fg_color=("#ff6b6b", "#d32f2f"),
                                               hover_color=("#ff5252", "#b71c1c"),
                                               command=lambda fid=filter_id, frame=first_frame:
                                               self.remove_specific_value_row(fid, frame))
                    remove_btn.pack(side="left", padx=(5, 0))

            # Обновляем окно для корректного отображения
            self.filters_scroll.update_idletasks()
            self.apply_filter_condition(filter_id)

    def remove_specific_value_row(self, filter_id, row_frame):
        """Удаляет конкретную строку с условием"""
        if 0 <= filter_id < len(self.filter_conditions):
            widgets = self.filter_conditions[filter_id]['widgets']
            if widgets['value_count'] > 1:
                # Находим и удаляем строку
                for i, row in enumerate(widgets['value_rows']):
                    if row['frame'] == row_frame:
                        # Удаляем из списка
                        widgets['value_rows'].pop(i)
                        widgets['value_count'] -= 1

                        # Удаляем фрейм
                        row_frame.destroy()

                        # Обновляем индексы оставшихся строк
                        for j, remaining_row in enumerate(widgets['value_rows']):
                            remaining_row['row_index'] = j

                        # Если после удаления осталась только одна строка, убираем кнопку удаления у нее
                        if widgets['value_count'] == 1 and len(widgets['value_rows']) > 0:
                            first_row = widgets['value_rows'][0]
                            first_frame = first_row['frame']
                            # Удаляем кнопку удаления у первой строки
                            for child in first_frame.winfo_children():
                                if isinstance(child, ctk.CTkButton) and child.cget("text") == "✕":
                                    child.destroy()
                                    break

                        break

                # Обновляем и применяем фильтр
                self.apply_filter_condition(filter_id)

                # Обновляем окно
                self.filters_scroll.update_idletasks()

    def remove_value_row(self, filter_id):
        """Удаляет последнюю строку с условием"""
        if 0 <= filter_id < len(self.filter_conditions):
            widgets = self.filter_conditions[filter_id]['widgets']
            if widgets['value_count'] > 1:
                # Находим и удаляем последнюю строку
                last_row = widgets['value_rows'][-1]
                last_frame = last_row['frame']

                # Удаляем из списка
                widgets['value_rows'].pop()
                widgets['value_count'] -= 1

                # Удаляем фрейм
                last_frame.destroy()

                # Если после удаления осталась только одна строка, убираем кнопку удаления у нее
                if widgets['value_count'] == 1 and len(widgets['value_rows']) > 0:
                    first_row = widgets['value_rows'][0]
                    first_frame = first_row['frame']
                    # Удаляем кнопку удаления у первой строки
                    for child in first_frame.winfo_children():
                        if isinstance(child, ctk.CTkButton) and child.cget("text") == "✕":
                            child.destroy()
                            break

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
                                    grandchild.configure(text=f"Фильтр #{i + 1}: {col_name}")
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

    def update_filter_columns(self):
        """Обновляет список колонок во всех фильтрах"""
        if not self.all_columns:
            return

        for condition in self.filter_conditions:
            widgets = condition['widgets']
            if 'col_combo' in widgets:
                current_value = widgets['col_var'].get()
                widgets['col_combo'].configure(values=self.all_columns)
                # Если текущее значение не в списке, устанавливаем первое значение
                if current_value not in self.all_columns and self.all_columns:
                    widgets['col_var'].set(self.all_columns[0])

    def build_query(self):
        """Строит MongoDB запрос из условий фильтрации"""
        if not self.filter_conditions:
            return {}

        filter_parts = []
        filter_logic_operators = []

        for i, condition in enumerate(self.filter_conditions):
            widgets = condition['widgets']

            col = widgets['col_var'].get()
            filter_logic_var = widgets['logic_var']

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

                # Сохраняем логический оператор для связи с предыдущим фильтром
                if filter_logic_var:
                    logic = filter_logic_var.get()
                else:
                    logic = "И"  # Для первого фильтра

                filter_logic_operators.append(logic)

        # Если нет условий, возвращаем пустой запрос
        if not filter_parts:
            return {}

        # Собираем итоговый запрос
        final_query = filter_parts[0]  # Начинаем с первого условия

        for i in range(1, len(filter_parts)):
            logic = filter_logic_operators[i]
            next_condition = filter_parts[i]

            # Преобразуем логический оператор в MongoDB оператор
            if logic == "И":
                # Для И объединяем с $and
                if "$and" not in final_query:
                    final_query = {"$and": [final_query, next_condition]}
                else:
                    final_query["$and"].append(next_condition)
            elif logic == "ИЛИ":
                # Для ИЛИ объединяем с $or
                if "$or" not in final_query:
                    final_query = {"$or": [final_query, next_condition]}
                else:
                    final_query["$or"].append(next_condition)
            elif logic == "НЕ":
                # Для НЕ используем $not
                final_query = {"$and": [final_query, {"$not": next_condition}]}
            elif logic == "НИ":
                # Для НИ используем $nor
                if "$nor" not in final_query:
                    final_query = {"$nor": [final_query, next_condition]}
                else:
                    final_query["$nor"].append(next_condition)

        # Глобальный поиск
        search_value = self.search_entry.get().strip()
        if search_value:
            or_conditions = []
            for col in self.all_columns:
                or_conditions.append({col: {"$regex": search_value, "$options": "i"}})

            if or_conditions:
                if final_query:
                    final_query = {"$and": [final_query, {"$or": or_conditions}]}
                else:
                    final_query = {"$or": or_conditions}

        return final_query

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
        """Строит одно условие для MongoDB"""
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

    def create_table_panel(self, parent):
        table_container = ctk.CTkFrame(
            parent,
            corner_radius=15
        )
        table_container.pack(side="left", fill="both", expand=True, padx=5, pady=5)

        controls_container = ctk.CTkFrame(table_container, fg_color="transparent")
        controls_container.pack(fill="x", padx=15, pady=10)

        search_frame = ctk.CTkFrame(controls_container, fg_color="transparent")
        search_frame.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(search_frame, text="Поиск:").pack(side="left", padx=(0, 5))
        self.search_entry = ctk.CTkEntry(search_frame, width=200, height=32)
        self.search_entry.pack(side="left", padx=(0, 5))
        self.search_entry.bind("<Return>", lambda e: self.apply_search())

        ctk.CTkButton(search_frame, text="🔍", width=40, height=32,
                      command=self.apply_search).pack(side="left")

        page_size_frame = ctk.CTkFrame(controls_container, fg_color="transparent")
        page_size_frame.pack(side="right")

        ctk.CTkLabel(page_size_frame, text="Строк на странице:").pack(side="left", padx=(0, 5))
        self.page_size_var = ctk.StringVar(value="100")
        page_size_combo = ctk.CTkComboBox(page_size_frame,
                                          values=["50", "100", "200", "500", "1000"],
                                          variable=self.page_size_var,
                                          width=80,
                                          height=32,
                                          command=self.change_page_size)
        page_size_combo.pack(side="left")

        table_frame = ctk.CTkFrame(
            table_container,
            corner_radius=12,
            border_width=1,
            border_color="#3a3a3a"
        )
        table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))

        self.create_treeview(table_frame)

    def create_treeview(self, parent):
        table_inner_frame = ctk.CTkFrame(parent, fg_color="#2b2b2b")
        table_inner_frame.pack(fill="both", expand=True, padx=1, pady=1)

        self.tree = ttk.Treeview(table_inner_frame, show='headings')

        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Treeview",
                        background="#2b2b2b",
                        foreground="white",
                        fieldbackground="#2b2b2b",
                        borderwidth=0,
                        relief="flat",
                        rowheight=35)

        style.configure("Treeview.Heading",
                        background="#3a3a3a",
                        foreground="white",
                        relief="flat",
                        borderwidth=0,
                        padding=(5, 15))

        style.map("Treeview.Heading",
                  background=[('active', '#4a4a4a')],
                  relief=[('pressed', 'flat'), ('active', 'flat')])

        vsb = ttk.Scrollbar(table_inner_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_inner_frame, orient="horizontal", command=self.tree.xview)

        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew", padx=(0, 0), pady=(0, 0))
        vsb.grid(row=0, column=1, sticky="ns", padx=(0, 0), pady=(0, 0))
        hsb.grid(row=1, column=0, sticky="ew", padx=(0, 0), pady=(0, 0))

        table_inner_frame.grid_rowconfigure(0, weight=1)
        table_inner_frame.grid_rowconfigure(1, weight=0)
        table_inner_frame.grid_columnconfigure(0, weight=1)
        table_inner_frame.grid_columnconfigure(1, weight=0)

        self.root.bind("<Configure>", lambda e: self.auto_adjust_columns())
        self.tree.bind("<Configure>", lambda e: self.auto_adjust_columns())
        self.tree.bind("<Double-1>", self.on_column_click)

    def create_aggregation_panel(self, parent):
        agg_container = ctk.CTkFrame(parent)
        agg_container.pack(side="bottom", fill="x", padx=5, pady=(5, 0))

        agg_header = ctk.CTkFrame(agg_container, fg_color="transparent")
        agg_header.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(agg_header, text="📊 Агрегация данных",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(side="left")

        agg_controls = ctk.CTkFrame(agg_container, fg_color="transparent")
        agg_controls.pack(fill="x", padx=10, pady=5)

        # Группировка
        ctk.CTkLabel(agg_controls, text="Группировать по:").pack(side="left", padx=(0, 5))
        self.group_by_var = ctk.StringVar(value="")
        self.group_by_combo = ctk.CTkComboBox(agg_controls,
                                              values=[],
                                              variable=self.group_by_var,
                                              width=150,
                                              height=32)
        self.group_by_combo.pack(side="left", padx=(0, 20))

        # Агрегационная функция
        ctk.CTkLabel(agg_controls, text="Функция:").pack(side="left", padx=(0, 5))
        self.agg_func_var = ctk.StringVar(value="")
        agg_func_combo = ctk.CTkComboBox(agg_controls,
                                         values=["сумма", "среднее", "минимум", "максимум",
                                                 "первое значение", "последнее значение", "все значения",
                                                 "уникальные значения", "количество", "выборочная дисперсия",
                                                 "генерируемая дисперсия"],
                                         variable=self.agg_func_var,
                                         width=200,
                                         height=32)
        agg_func_combo.pack(side="left", padx=(0, 5))

        # Колонка для агрегации
        ctk.CTkLabel(agg_controls, text="Колонка:").pack(side="left", padx=(0, 5))
        self.agg_col_var = ctk.StringVar(value="")
        self.agg_col_combo = ctk.CTkComboBox(agg_controls,
                                             values=[],
                                             variable=self.agg_col_var,
                                             width=150,
                                             height=32)
        self.agg_col_combo.pack(side="left", padx=(0, 20))

        # Кнопки управления агрегацией
        agg_buttons = ctk.CTkFrame(agg_controls, fg_color="transparent")
        agg_buttons.pack(side="left")

        ctk.CTkButton(agg_buttons, text="Применить", width=100, height=32,
                      command=self.apply_aggregation).pack(side="left", padx=2)
        ctk.CTkButton(agg_buttons, text="Сбросить", width=100, height=32,
                      command=self.reset_aggregation).pack(side="left", padx=2)

    def create_bottom_panel(self, parent):
        bottom_frame = ctk.CTkFrame(parent, height=50)
        bottom_frame.pack(fill="x", padx=0, pady=(5, 0))

        self.info_label = ctk.CTkLabel(bottom_frame, text="Загрузка...")
        self.info_label.pack(side="left", padx=20)

        pagination_frame = ctk.CTkFrame(bottom_frame, fg_color="transparent")
        pagination_frame.pack(side="right", padx=20)

        ctk.CTkButton(pagination_frame, text="⏮ Первая", width=80, height=32,
                      command=lambda: self.change_page(0)).pack(side="left", padx=2)
        ctk.CTkButton(pagination_frame, text="◀ Назад", width=80, height=32,
                      command=self.prev_page).pack(side="left", padx=2)

        self.page_label = ctk.CTkLabel(pagination_frame, text="Страница 1 из 1")
        self.page_label.pack(side="left", padx=10)

        ctk.CTkButton(pagination_frame, text="Вперед ▶", width=80, height=32,
                      command=self.next_page).pack(side="left", padx=2)
        ctk.CTkButton(pagination_frame, text="Последняя ⏭", width=80, height=32,
                      command=self.last_page).pack(side="left", padx=2)

        page_entry_frame = ctk.CTkFrame(bottom_frame, fg_color="transparent")
        page_entry_frame.pack(side="left", padx=10)

        ctk.CTkLabel(page_entry_frame, text="Перейти:").pack(side="left", padx=5)
        self.page_entry = ctk.CTkEntry(page_entry_frame, width=50, height=32)
        self.page_entry.pack(side="left", padx=5)
        self.page_entry.bind("<Return>", lambda e: self.go_to_specific_page())

    def auto_adjust_columns(self, event=None):
        if not self.all_columns or not self.tree.winfo_exists():
            return

        tree_width = self.tree.winfo_width()

        if tree_width > 100 and self.all_columns:
            num_columns = len(self.all_columns)
            available_width = tree_width - 20

            if num_columns > 0:
                col_width = max(150, available_width // num_columns)

                for col in self.all_columns:
                    self.tree.column(col, width=col_width, anchor="w", stretch=False)

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
            sample = self.collection.find_one()
            if sample:
                self.all_columns = [col for col in sample.keys() if col != '_id']

                cursor = self.collection.find({}, {'_id': 0}).limit(1000)
                records = list(cursor)

                if records:
                    df_sample = pd.DataFrame(records)
                    for col in self.all_columns:
                        if col in df_sample.columns:
                            dtype = str(df_sample[col].dtype)
                            self.column_types[col] = dtype

                            if df_sample[col].nunique() < 100:
                                unique_vals = df_sample[col].dropna().unique().tolist()
                                unique_vals_str = [str(val) for val in unique_vals]
                                self.unique_values_cache[col] = sorted(unique_vals_str)[:50]

                # Обновляем комбобоксы
                if self.all_columns:
                    self.group_by_combo.configure(values=self.all_columns)
                    self.agg_col_combo.configure(values=self.all_columns)

        except Exception as e:
            print(f"Ошибка определения схемы: {e}")

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
                if mongo_func in ["$sum", "$avg", "$min", "$max", "$first", "$last",
                                  "$push", "$addToSet", "$stdDevPop", "$stdDevSamp"]:
                    group_stage["result"] = {mongo_func: f"${agg_col}"}
            else:
                # Если колонка не выбрана, но функция требует ее
                if mongo_func not in ["$count"]:
                    messagebox.showwarning("Предупреждение",
                                           "Выберите колонку для агрегации")
                    return

            pipeline.append({"$group": group_stage})

            # Сортировка по результату агрегации
            sort_direction = self.sort_direction if self.sort_column else 1
            if self.sort_column:
                pipeline.append({"$sort": {self.sort_column: sort_direction}})
            else:
                pipeline.append({"$sort": {"_id": 1}})

            # Выполняем агрегацию
            result = list(self.collection.aggregate(pipeline))

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
        # Очищаем таблицу
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Настраиваем колонки для агрегации
        columns = [group_by]
        if agg_func == "количество":
            columns.append("Количество")
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
            columns.append(f"{func_display}({agg_col})")

        self.tree["columns"] = columns

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=200, anchor="w", minwidth=150)

        # Добавляем данные
        for i, record in enumerate(results):
            values = []
            for col in columns:
                if col == group_by:
                    val = record.get("_id", "N/A")
                elif col == "Количество" or "count" in record:
                    val = record.get("count", 0)
                else:
                    val = record.get("result", 0)

                if isinstance(val, float):
                    val = f"{val:.4f}" if agg_func in ["выборочная дисперсия",
                                                       "генерируемая дисперсия"] else f"{val:.2f}"
                elif isinstance(val, list):
                    val = f"[{len(val)} значений]"
                elif val is None:
                    val = "N/A"
                else:
                    val = str(val)
                values.append(val)

            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            self.tree.insert('', 'end', values=values, tags=(tag,))

        self.tree.tag_configure('evenrow', background='#2b2b2b')
        self.tree.tag_configure('oddrow', background='#3a3a3a')

        # Обновляем информацию
        self.info_label.configure(text=f"Агрегировано {len(results)} групп")
        self.page_label.configure(text="Агрегация")
        self.page_entry.delete(0, 'end')

        # Автонастройка колонок
        self.auto_adjust_columns()

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
            self.total_records = self.collection.count_documents(query)

            total_all = self.collection.count_documents({})
            self.records_count_label.configure(
                text=f"Найдено: {self.total_records:,} из {total_all:,} записей"
            )

            self.load_page_data()
            self.update_info()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка загрузки данных: {str(e)}")
            import traceback
            traceback.print_exc()

    def load_page_data(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        if self.all_columns:
            self.tree["columns"] = self.all_columns

            for col in self.all_columns:
                non_null_count = self.collection.count_documents({col: {"$ne": None, "$exists": True}})
                total_count = self.collection.count_documents({})

                header_text = f"{col}\n({non_null_count:,}/{total_count:,})"
                self.tree.heading(col, text=header_text,
                                  command=lambda c=col: self.treeview_sort(c))
                self.tree.column(col, width=200, anchor="w", minwidth=150, stretch=False)

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

            for i, record in enumerate(cursor):
                values = []
                for col in self.all_columns:
                    val = record.get(col, '')
                    if isinstance(val, float):
                        val = f"{val:.2f}"
                    elif val is None:
                        val = "[ПУСТО]"
                    else:
                        val = str(val)
                    values.append(val)

                tag = 'evenrow' if i % 2 == 0 else 'oddrow'
                self.tree.insert('', 'end', values=values, tags=(tag,))

            self.tree.tag_configure('evenrow', background='#2b2b2b')
            self.tree.tag_configure('oddrow', background='#3a3a3a')

            self.auto_adjust_columns()

        except Exception as e:
            print(f"Ошибка загрузки данных: {e}")

    def update_info(self):
        if self.aggregation_mode:
            return

        total_pages = max(1, (self.total_records + self.page_size - 1) // self.page_size)
        current_page = min(self.current_page + 1, total_pages)

        start_rec = self.current_page * self.page_size + 1
        end_rec = min((self.current_page + 1) * self.page_size, self.total_records)

        info_text = f"Показано {start_rec}-{end_rec} из {self.total_records:,} записей"
        if self.total_records > 0:
            percentage = (self.total_records / self.collection.count_documents({})) * 100
            info_text += f" ({percentage:.1f}% от общей базы)"

        self.info_label.configure(text=info_text)
        self.page_label.configure(text=f"Страница {current_page} из {total_pages}")
        self.page_entry.delete(0, 'end')
        self.page_entry.insert(0, str(current_page))

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

    def go_to_specific_page(self):
        try:
            page_num = int(self.page_entry.get()) - 1
            self.change_page(page_num)
        except:
            messagebox.showwarning("Предупреждение", "Введите корректный номер страницы")

    def apply_search(self):
        self.current_page = 0
        self.load_data()

    def apply_sort(self, column, direction):
        self.sort_column = column
        self.sort_direction = direction
        self.load_data()

    def treeview_sort(self, column):
        if self.sort_column == column:
            self.sort_direction *= -1
        else:
            self.sort_column = column
            self.sort_direction = 1

        self.load_data()

    def on_column_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            column = self.tree.identify_column(event.x)
            col_index = int(column.replace('#', '')) - 1
            if 0 <= col_index < len(self.all_columns):
                self.apply_sort(self.all_columns[col_index],
                                -1 if self.sort_direction == 1 else 1)

    def clear_all_filters(self):
        # Очищаем все условия во всех фильтрах (оставляем только по одному пустому условию)
        for condition in self.filter_conditions:
            widgets = condition['widgets']

            # Оставляем только первую строку
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

            # Убираем кнопку удаления у первой строки если она есть
            if widgets['value_rows']:
                first_frame = widgets['value_rows'][0]['frame']
                for child in first_frame.winfo_children():
                    if isinstance(child, ctk.CTkButton) and child.cget("text") == "✕":
                        child.destroy()
                        break

        self.search_entry.delete(0, 'end')
        self.sort_column = None
        self.sort_direction = 1
        self.current_page = 0
        self.load_data()

    def show_statistics(self):
        try:
            query = self.build_query()
            cursor = self.collection.find(query, {'_id': 0})
            data = list(cursor)

            if not data:
                messagebox.showinfo("Статистика", "Нет данных для отображения статистики")
                return

            df = pd.DataFrame(data)

            stats_window = ctk.CTkToplevel(self.root)
            stats_window.title("Расширенная статистика")
            stats_window.geometry("1000x700")

            notebook = ttk.Notebook(stats_window)
            notebook.pack(fill="both", expand=True, padx=10, pady=10)

            # Общая статистика
            general_frame = ctk.CTkFrame(notebook)
            notebook.add(general_frame, text="Общая")

            text_widget = ctk.CTkTextbox(general_frame, wrap="word")
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)

            total_all = self.collection.count_documents({})

            stats_text = "СТАТИСТИКА ДАННЫХ NISSAN\n"
            stats_text += "=" * 60 + "\n\n"
            stats_text += f"Всего записей в фильтре: {len(df):,}\n"
            stats_text += f"Всего записей в базе: {total_all:,}\n"
            stats_text += f"Процент отображения: {(len(df) / total_all * 100):.1f}%\n\n"

            # Статистика по типам данных
            stats_text += "ТИПЫ ДАННЫХ:\n"
            for col in df.columns:
                dtype = str(df[col].dtype)
                stats_text += f"  {col}: {dtype}\n"
            stats_text += "\n"

            stats_text += "=" * 60 + "\n\n"

            # Детальная статистика по столбцам
            for col in df.columns:
                non_null = df[col].notna().sum()
                null_count = df[col].isna().sum()
                unique = df[col].nunique()
                stats_text += f"{col}:\n"
                stats_text += f"  Непустых значений: {non_null:,}\n"
                stats_text += f"  Пустых значений: {null_count:,}\n"
                stats_text += f"  Уникальных значений: {unique:,}\n"
                if non_null > 0:
                    stats_text += f"  Заполненность: {(non_null / len(df) * 100):.1f}%\n"

                if df[col].dtype in ['int64', 'float64']:
                    stats_text += f"  Минимум: {df[col].min():.2f}\n"
                    stats_text += f"  Максимум: {df[col].max():.2f}\n"
                    stats_text += f"  Среднее: {df[col].mean():.2f}\n"
                    stats_text += f"  Медиана: {df[col].median():.2f}\n"
                    stats_text += f"  Стандартное отклонение: {df[col].std():.2f}\n"
                    stats_text += f"  Дисперсия: {df[col].var():.2f}\n"

                stats_text += "\n"

            text_widget.insert("1.0", stats_text)
            text_widget.configure(state="disabled")

            # Статистика корреляции
            if len(df.select_dtypes(include=['float64', 'int64']).columns) > 1:
                corr_frame = ctk.CTkFrame(notebook)
                notebook.add(corr_frame, text="Корреляции")

                corr_text = ctk.CTkTextbox(corr_frame, wrap="word")
                corr_text.pack(fill="both", expand=True, padx=10, pady=10)

                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                corr_matrix = df[numeric_cols].corr()

                corr_text.insert("1.0", "КОРРЕЛЯЦИОННАЯ МАТРИЦА:\n")
                corr_text.insert("2.0", "=" * 50 + "\n\n")
                corr_text.insert("3.0", str(corr_matrix))
                corr_text.configure(state="disabled")

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка расчета статистики: {str(e)}")

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = EnhancedNissanGUI()
    app.run()