import gspread
import json
import time
import re
import os
import requests
from datetime import datetime, timedelta
from openai import OpenAI

# ============================================
# НАСТРОЙКИ (GITHUB SECRETS)
# ============================================
try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    PHP_SECRET_KEY = os.environ["PHP_SECRET_KEY"]
    
    creds_json = os.environ["GSPREAD_CREDS"]
    creds_dict = json.loads(creds_json)
except KeyError as e:
    print(f"🔴 CRITICAL: Не найден секрет {e}!")
    exit(1)

# --- КОНФИГУРАЦИЯ ---
BITRIX_WEBHOOK = "https://bitrix.emet.in.ua/rest/2049/hx8tyfl6nkj5kluk/"
PHP_ENDPOINT = "https://bitrix.emet.in.ua/get_chat_id.php"

SHEET_NAME = "BitrixChat"
WORKSHEET_DATA = "Auto_Monitoring"   # Сюда пишем данные
WORKSHEET_CONFIG = "System_Config"   # Отсюда берем дату старта

AI_MODEL = "gpt-4o"
MIN_MESSAGES_COUNT = 2 

# Глобальные переменные для справочников
STATUS_MAP = {}
SOURCE_MAP = {}

# Менеджеры
MANAGER_NAMES = ["Яна Наконечна", "Софія Кривенко", "Влада Шарай", "Анастасия Другтейн"]
MANAGER_IDS_INT = [1519, 2077, 6894, 13408]

# --- СЛОВАРИ ДЛЯ ДЕТЕКТОРОВ ---
B2B_KEYWORDS = [
    # Навчання
    "розклад семінарів", "расписание семинаров", 
    "запис на семінар", "запись на семинар",
    "навчання косметологів", "обучение косметологов",
    
    # Прайси/Умови
    "прайс косметолога", "прайс для косметологов", "прайс для косметологів",
    "умови співпраці", "условия сотрудничества",
    "оптовий", "оптовый", "гуртовий", # Замість просто "опт"
    
    # Ідентифікація
    "я косметолог", "я лікар", "я врач", "ми клініка", "мы клиника", "ми салон",
    "кабінет косметолога", "кабинет косметолога",
    
    # Документи (лише конкретні фрази)
    "надіслати диплом", "отправить диплом", "фото диплома", 
    
    
    # Проф. бренди (Тут безпечно)
    "neuramis", "нейраміс", "medytox", "медитокс", "neuronox", "нейронокс",
    
    # Інфлюенс
    "блогер", "blogger", "бартер", "barter", "рекламна інтеграція"
]

B2B_NAMES = ["dr", "dr.", "лікар", "врач", "косметолог", "dermatolog", "cosmetolog", "clinic", "клініка", "клиника", "md", "estet"]
REFERRAL_KEYWORDS = ["порадьте косметолога", "посоветуйте", "де зробити", "контакти лікаря", "записатись на процедуру", "уколоть"]
CLOSE_WORDS = ["ттн", "накладна", "номер накладної", "дякуємо за замовлення", "оформлено", "реквізити", "оплату отримали"]
BRAND_EMOJIS = ["🌿", "🍃", "☘️", "🌱", "🍀", "💰", "✨", "💫", "🛒", "🛍", "💚", "🤍", "💧", "☺️", "🙌🏻", "🥰", "💌"]
DISCOUNT_WORDS = ["знижка", "скидка", "парна", "парная", "від 2", "от 2", "набір", "набор", "курс", "15%", "-%"]

client = OpenAI(api_key=OPENAI_API_KEY)

# ============================================
# 1. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (БИТРИКС)
# ============================================

def load_dictionaries():
    """Скачиваем красивые названия статусов и источников"""
    print("📥 Загрузка справочников Битрикс...")
    try:
        res = requests.post(f"{BITRIX_WEBHOOK}crm.status.list", json={"filter": {"ENTITY_ID": "STATUS"}}).json()
        for item in res.get('result', []): STATUS_MAP[item['STATUS_ID']] = item['NAME']
        
        res = requests.post(f"{BITRIX_WEBHOOK}crm.status.list", json={"filter": {"ENTITY_ID": "SOURCE"}}).json()
        for item in res.get('result', []): SOURCE_MAP[item['STATUS_ID']] = item['NAME']
        print("   [OK] Справочники готовы.")
    except Exception as e:
        print(f"   [ERROR] Ошибка справочников: {e}")

def check_real_deal(lead_id):
    """Честная проверка сделки через API"""
    try:
        res = requests.post(f"{BITRIX_WEBHOOK}crm.deal.list", json={
            "filter": {"LEAD_ID": lead_id},
            "select": ["ID"]
        }).json()
        deals = res.get('result', [])
        if deals:
            return f"Есть (ID: {deals[0]['ID']})"
    except: pass
    return "Нет"

def get_chat_id_via_php(session_id):
    try:
        res = requests.get(PHP_ENDPOINT, params={"session_id": session_id, "key": PHP_SECRET_KEY}, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if 'chat_id' in data: return data['chat_id']
    except: pass
    return None

def find_chat_id_ultimate(lead_id):
    try:
        res = requests.post(f"{BITRIX_WEBHOOK}imopenlines.crm.chat.get", json={"CRM_ENTITY_TYPE": "LEAD", "CRM_ENTITY": lead_id}).json()
        if res.get('result'): return f"chat{res['result'][0]['CHAT_ID']}"
    except: pass
    
    try:
        payload = {
            "filter": {"OWNER_ID": lead_id, "OWNER_TYPE_ID": 1, "PROVIDER_ID": "IMOPENLINES_SESSION"},
            "select": ["ID", "PROVIDER_PARAMS", "ASSOCIATED_ENTITY_ID"],
            "order": {"ID": "DESC"}
        }
        res = requests.post(f"{BITRIX_WEBHOOK}crm.activity.list", json=payload).json()
        activities = res.get('result', [])
        
        for act in activities:
            params = act.get('PROVIDER_PARAMS', {})
            if isinstance(params, str) and params:
                try: params = json.loads(params)
                except: pass
            
            if isinstance(params, dict):
                if 'chatId' in params: return f"chat{params['chatId']}"
                if 'CHAT_ID' in params: return f"chat{params['CHAT_ID']}"
            
            session_id = act.get('ASSOCIATED_ENTITY_ID')
            if session_id:
                recovered_id = get_chat_id_via_php(session_id)
                if recovered_id: return f"chat{recovered_id}"
    except: pass
    return None

def clean_text_for_google(text):
    if not text: return ""
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
    return text

def get_chat_text(lead_id):
    dialog_id = find_chat_id_ultimate(lead_id)
    if not dialog_id: return None
    try:
        res_msg = requests.post(f"{BITRIX_WEBHOOK}im.dialog.messages.get", json={"DIALOG_ID": dialog_id, "LIMIT": 100}).json()
        messages = res_msg.get('result', {}).get('messages', [])
        
        if len(messages) < MIN_MESSAGES_COUNT: return None
        
        users_dict = res_msg.get('result', {}).get('users', [])
        user_names = {}
        for u in users_dict:
            name = u.get('name', '').strip()
            last = u.get('last_name', '').strip()
            user_names[u['id']] = name if last in name else f"{name} {last}".strip()

        clean_dialog = []
        messages.sort(key=lambda x: x['id'])
        
        has_text = False
        for msg in messages:
            if msg['author_id'] == 0 or not msg.get('text'): continue
            author_name = user_names.get(msg['author_id'], "Клиент")
            clean_t = re.sub(r'\[.*?\]', '', msg['text']).replace('&quot;', '"').strip()
            clean_t = clean_text_for_google(clean_t)
            clean_dialog.append(f"{author_name}: {clean_t}")
            has_text = True
        
        if not has_text: return None
        return "\n".join(clean_dialog)
    except: return None

# ============================================
# 2. ЛОГИКА АНАЛИЗА (AI + ДЕТЕКТОРЫ)
# ============================================

def load_ai_config():
    return {
            "SUPPLEMENTS": {"no_discount": 10, "no_description": 10},
            "COSMETICS": {"no_emoji": 10, "no_cross_sell": 10},
            "GENERAL": {"no_question": 5, "stop_word_na_zhal": 15, "gave_up_on_objection": 10}
        }
CONFIG = load_ai_config()

def check_manager_presence(text):
    for name in MANAGER_NAMES:
        if name in text: return True
    return False

def check_is_b2b_python(text, client_name):
    text_lower = text.lower()
    name_lower = client_name.lower()
    for word in B2B_KEYWORDS:
        if word in text_lower: return True
    for title in B2B_NAMES:
        if title in name_lower: return True
    if "iuse" in text_lower and not ("collagen" in text_lower or "колаген" in text_lower): return True
    return False

def check_keywords(text, keywords):
    text_lower = text.lower()
    for word in keywords:
        if word in text_lower: return True
    return False

def check_emojis_presence(text):
    for icon in BRAND_EMOJIS:
        if icon in text: return True
    return False

def check_question_presence(text):
    tail = text[-200:].strip()
    if "?" in tail or "?" in text[-50:]: return True
    return False

def check_deal_closed_text(text):
    text_lower = text.lower()[-400:] 
    for word in CLOSE_WORDS:
        if word in text_lower: return True
    return False

def check_is_supplement(text):
    text_lower = text.lower()
    if "magnox" in text_lower or "saffrox" in text_lower: return True
    if "iuse" in text_lower and ("collagen" in text_lower or "колаген" in text_lower): return True
    return False

def check_discount_presence(text):
    text_lower = text.lower()
    for w in DISCOUNT_WORDS:
        if w in text_lower: return True
    return False

# === ИСПРАВЛЕННЫЙ ГЕНЕРАТОР ПРОМПТА ===
def generate_prompt(has_emojis, has_question, is_closed_text, is_suppl, has_discount, mode):
    
    # --- СЦЕНАРІЙ B2B ---
    if mode == "B2B":
        return """
Ти — Експерт з комунікацій. Це діалог B2B (лікар/партнер).
Твоє завдання: Оцінити тон і ввічливість. Оцінку продажів (Score) ставити 0.
JSON: {"product_type": "B2B", "score": 0, "summary": "...", "good_points": "...", "bad_points": "-", "recommendation": "-", "sales_feedback": "..."}
"""

    # --- СЦЕНАРІЙ B2C SALES ---
    type_instr = "СИСТЕМА: Це БАДи. Оцінюй як SUPPLEMENTS." if is_suppl else "Визнач категорію (COSMETICS або SUPPLEMENTS)."
    emoji_instr = "СИСТЕМА: Емодзі є." if has_emojis else "СИСТЕМА: Емодзі немає."
    discount_instr = "СИСТЕМА: Знижку знайдено в тексті. Штрафувати заборонено." if has_discount else "СИСТЕМА: Згадок про знижку не знайдено."
    
    sales_status = "СИСТЕМА: Угода закрита (ТТН). Успіх." if is_closed_text else "СИСТЕМА: Угода НЕ закрита."
    
    question_instr = "СИСТЕМА: Питання немає."
    if has_question: question_instr = "СИСТЕМА: Знак питання є."
    elif is_closed_text: question_instr = "СИСТЕМА: Діалог завершено замовленням. Питання не потрібне."

    # Бали
    pen_s_disc = CONFIG["SUPPLEMENTS"]["no_discount"]
    pen_s_desc = CONFIG["SUPPLEMENTS"]["no_description"]
    pen_c_emoji = CONFIG["COSMETICS"]["no_emoji"]
    pen_c_cross = CONFIG["COSMETICS"]["no_cross_sell"]
    pen_g_quest = CONFIG["GENERAL"]["no_question"]
    pen_g_stop = CONFIG["GENERAL"]["stop_word_na_zhal"]
    pen_g_giveup = CONFIG["GENERAL"]["gave_up_on_objection"]

    return f"""
Ти — Досвідчений Керівник Відділу Продажів (РОП).

ФАКТИ (ВРАХУЙ ЇХ):
1. {type_instr}
2. {sales_status}
3. {emoji_instr}
4. {question_instr}
5. {discount_instr}

АЛГОРИТМ ОЦІНКИ B2C (Початково 100 балів):

1. ВИЗНАЧ СЦЕНАРІЙ ДІАЛОГУ:
   - Сценарій А (Інтерес): Клієнт запитує ціну, погоджується або мовчить.
   - Сценарій Б (Заперечення): Клієнт пише "Ні", "Дорого", "Подумаю".

2. РОЗРАХУНОК ШТРАФІВ:

   🔴 БАДи (SUPPLEMENTS):
   - ЗНИЖКА: Дивись ФАКТ №5. 
     - Якщо система каже, що знижка є -> ОК.
     - Якщо немає -> Мінус {pen_s_disc}. (Bad: "Не запропоновано знижку на курс").
   - ОПИС: Є опис користі ПЕРЕД ціною? НІ -> Мінус {pen_s_desc}.
   - ЕМОДЗІ: ІГНОРУЙ ПОВНІСТЮ.

   🟢 КОСМЕТИКА (COSMETICS):
   - ЕМОДЗІ: Дивись ФАКТ №3. Немає -> Мінус {pen_c_emoji}. (Bad: "Відсутні фірмові емодзі").
   - CROSS-SELL: 
     - Якщо Сценарій А (Інтерес) -> Немає? Мінус {pen_c_cross}.
     - Якщо Сценарій Б (Заперечення) -> Cross-sell НЕ вимагається.

   ⚫ ЗАГАЛЬНІ:
   - РОБОТА З ЗАПЕРЕЧЕННЯМ (Тільки Сценарій Б):
     - Здався ("Ок")? -> Мінус {pen_g_giveup}.
     - Спробував відпрацювати або Soft Exit? -> ОК (0 штрафу).
   - ЗАПИТАННЯ: Дивись ФАКТ №4. (Немає і не закрито -> Мінус {pen_g_quest}).
   - СТОП-СЛОВА: "На жаль"? ТАК -> Мінус {pen_g_stop}.

3. ЕКСПЕРТНИЙ ВИСНОВОК РОПа (Sales Feedback):
   - Напиши розгорнутий, живий відгук про якість роботи менеджера.
   - Оціни: Ініціативу, Експертність, Емпатію.
   - Як відпрацьовано заперечення (якщо були)?
   - Чи був персональний підхід?

ФОРМАТ JSON:
{{
  "product_type": "COSMETICS" / "SUPPLEMENTS",
  "score": (число),
  "summary": "Стислий зміст",
  "good_points": "Текст",
  "bad_points": "Текст",
  "recommendation": "Текст",
  "sales_feedback": "Твій експертний коментар"
}}
"""

def analyze_row(dialog_text, client_name):
    if not dialog_text or len(dialog_text) < 5: return None
    
    is_b2b_python = check_is_b2b_python(dialog_text, client_name)
    if is_b2b_python: mode = "B2B"
    else:
        if check_keywords(dialog_text, REFERRAL_KEYWORDS):
            return {"product_type": "B2C_REFERRAL", "score": 0, "summary": "Пошук лікаря", "good_points": "-", "bad_points": "-", "recommendation": "-", "sales_feedback": "Технічний запит"}
        
        has_manager = check_manager_presence(dialog_text)
        if not has_manager:
            return {"product_type": "NO_REPLY", "score": 0, "summary": "Без відповіді", "good_points": "-", "bad_points": "Ігнорування", "recommendation": "Відповісти", "sales_feedback": "Втрачений лід"}
        mode = "B2C"

    has_emojis = check_emojis_presence(dialog_text)
    has_question = check_question_presence(dialog_text) 
    is_closed = check_deal_closed_text(dialog_text)
    is_suppl = check_is_supplement(dialog_text)
    has_discount = check_discount_presence(dialog_text)
    
    final_prompt = generate_prompt(has_emojis, has_question, is_closed, is_suppl, has_discount, mode)
    
    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": final_prompt},
                {"role": "user", "content": f"Текст:\n{dialog_text}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        data = json.loads(response.choices[0].message.content)
        
        if mode == "B2B":
            data['score'] = 0
            data['recommendation'] = "-"
        elif mode == "B2C":
            bad = str(data.get('bad_points', ''))
            if bad in ["-", "", "None", "[]"] or len(bad) < 4 or "Не виявлено" in bad:
                data['score'] = 100
                data['bad_points'] = "-"
            if data['score'] == 0: data['score'] = 40

        return data
    except Exception as e:
        print(f"Error AI: {e}")
        return None

# ============================================
# 3. ГЛАВНЫЙ ЦИКЛ (SMART MONITORING)
# ============================================
def main():
    print(f"--- GITHUB SMART MONITORING (Date + Economy) ---")
    load_dictionaries()
    
    try:
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open(SHEET_NAME)
        ws_data = sh.worksheet(WORKSHEET_DATA)
        ws_conf = sh.worksheet(WORKSHEET_CONFIG)
        
        # 1. Загружаем КЭШ (чтобы не тратить деньги, если текст не изменился)
        print("📊 Читаем таблицу для кэширования...")
        all_values = ws_data.get_all_values()
        
        # Структура кэша: { "LEAD_ID": {"row_index": 5, "text": "...", "ai_data": [...] } }
        cache = {}
        for idx, row in enumerate(all_values):
            if idx == 0: continue 
            if len(row) > 0:
                lid = str(row[0])
                # Текст диалога обычно в колонке I (индекс 8)
                dialog_text = row[8] if len(row) > 8 else ""
                
                # Сохраняем AI данные (колонки J-P, индексы 9-15)
                # Чтобы при обновлении только статуса, мы могли вернуть старую оценку
                ai_data = row[9:16] if len(row) > 15 else [""]*7
                
                cache[lid] = {
                    "row_index": idx + 1,
                    "text": dialog_text,
                    "ai_data": ai_data
                }
        print(f"   В кэше {len(cache)} записей.")

    except Exception as e:
        print(f"🔴 Critical Error Google: {e}")
        return

    # 2. Читаем ДАТУ СТАРТА из конфига (Ячейка B2)
    start_date_raw = ws_conf.acell('B2').value
    if not start_date_raw:
        print("⚠️ В B2 пусто! Использую дату по умолчанию (вчера).")
        start_date_val = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # Пробуем распарсить разные форматы даты
        try:
            if "." in start_date_raw:
                dobj = datetime.strptime(start_date_raw, "%d.%m.%Y")
                start_date_val = dobj.strftime("%Y-%m-%d")
            else:
                start_date_val = start_date_raw
        except:
            print(f"⚠️ Ошибка даты '{start_date_raw}'. Беру вчера.")
            start_date_val = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 РЕЖИМ: Проверка всех лидов созданных после {start_date_val}")

    total_updated = 0
    total_new = 0
    total_skipped_ai = 0
    
    for mgr_id in MANAGER_IDS_INT:
        print(f"\n👤 Менеджер {mgr_id}...", end=" ")
        try:
            # Запрос лидов по дате
            payload = {
                "order": {"DATE_CREATE": "ASC"},
                "filter": {"ASSIGNED_BY_ID": mgr_id, ">DATE_CREATE": f"{start_date_val}T00:00:00"},
                "select": ["ID", "TITLE", "STATUS_ID", "DATE_CREATE", "NAME", "LAST_NAME", "SOURCE_ID"]
            }
            leads = requests.post(f"{BITRIX_WEBHOOK}crm.lead.list", json=payload).json().get('result', [])
            
            if not leads:
                print("Пусто.")
                continue

            print(f"Найдено {len(leads)} лидов за период.")

            for lead in leads:
                source_id = str(lead.get('SOURCE_ID', ''))
                if 'INSTAGRAM' not in source_id.upper(): continue
                
                lead_id_str = str(lead['ID'])
                
                # Скачиваем свежий текст
                new_chat_text = get_chat_text(lead['ID'])
                if not new_chat_text: continue
                
                # Подготовка данных (Статусы и Сделки - ВСЕГДА обновляем)
                readable_source = SOURCE_MAP.get(lead.get('SOURCE_ID'), lead.get('SOURCE_ID'))
                readable_status = STATUS_MAP.get(lead.get('STATUS_ID'), lead.get('STATUS_ID'))
                deal_info = check_real_deal(lead['ID']) # <-- ЧЕСТНАЯ ПРОВЕРКА
                client_name = f"{lead.get('NAME', '')} {lead.get('LAST_NAME', '')}".strip()
                link = f"https://bitrix.emet.in.ua/crm/lead/details/{lead['ID']}/"

                # ЛОГИКА СРАВНЕНИЯ (ЭКОНОМИЯ)
                need_ai_analysis = True
                cached_ai_result = []

                if lead_id_str in cache:
                    old_text = cache[lead_id_str]['text']
                    # Сравниваем тексты
                    if clean_text_for_google(new_chat_text) == clean_text_for_google(old_text):
                        # ТЕКСТ НЕ ИЗМЕНИЛСЯ -> AI НЕ НУЖЕН
                        need_ai_analysis = False
                        cached_ai_result = cache[lead_id_str]['ai_data']
                        total_skipped_ai += 1
                
                # Формируем строку
                ai_fields = []
                
                if need_ai_analysis:
                    # Запускаем AI (используя НОВЫЙ генератор промпта)
                    result = analyze_row(new_chat_text, client_name)
                    if result:
                        ai_fields = [
                            result.get('product_type'), result.get('score'), result.get('summary'),
                            str(result.get('good_points')), str(result.get('bad_points')),
                            result.get('recommendation'), result.get('sales_feedback')
                        ]
                    else:
                        ai_fields = ["ERROR", "-", "-", "-", "-", "-", "-"]
                else:
                    # Берем старые данные AI из кэша
                    ai_fields = cached_ai_result

                # Итоговая строка
                row_data = [
                    lead_id_str, lead['DATE_CREATE'][:10], mgr_id, client_name,
                    readable_source, readable_status, deal_info, link, new_chat_text[:45000]
                ] + ai_fields

                # ЗАПИСЬ В GOOGLE
                if lead_id_str in cache:
                    # ОБНОВЛЕНИЕ
                    r_idx = cache[lead_id_str]['row_index']
                    ws_data.update(range_name=f"A{r_idx}:P{r_idx}", values=[row_data])
                    total_updated += 1
                else:
                    # НОВАЯ ЗАПИСЬ
                    ws_data.append_row(row_data)
                    total_new += 1
                
                time.sleep(1.2) # Небольшая задержка для API

        except Exception as e:
            print(f"Err Manager {mgr_id}: {e}")

    # Финальный отчет в конфиг (Ячейка B1)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    info_msg = f"{now_str} | Check > {start_date_val} | New: {total_new}, Upd: {total_updated}, AI Saved: {total_skipped_ai}"
    ws_conf.update_acell('B1', info_msg)
    
    print(f"\n✅ [DONE] {info_msg}")

if __name__ == "__main__":
    main()
