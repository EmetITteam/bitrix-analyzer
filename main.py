import gspread
import json
import time
import re
import os
import requests
from datetime import datetime, timedelta
from openai import OpenAI

# ============================================
# НАЛАШТУВАННЯ (GITHUB SECRETS)
# ============================================
try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    PHP_SECRET_KEY = os.environ["PHP_SECRET_KEY"]
    
    creds_json = os.environ["GSPREAD_CREDS"]
    creds_dict = json.loads(creds_json)
except KeyError as e:
    print(f"🔴 CRITICAL: Не знайдено секрет {e}!")
    exit(1)

# --- КОНФІГУРАЦІЯ ---
BITRIX_WEBHOOK = "https://bitrix.emet.in.ua/rest/2049/hx8tyfl6nkj5kluk/"
PHP_ENDPOINT = "https://bitrix.emet.in.ua/get_chat_id.php"
CONFIG_FILE = "config.json"

SHEET_NAME = "BitrixChat"
WORKSHEET_DATA = "Auto_Monitoring"   # Лист для діалогів
WORKSHEET_CONFIG = "System_Config"   # Лист з датою

AI_MODEL = "gpt-4o"
MIN_MESSAGES_COUNT = 2  # <--- ИСПРАВЛЕНО: Минимальное кол-во сообщений

# 1. МЕНЕДЖЕРИ
MANAGER_NAMES = ["Яна Наконечна", "Софія Кривенко", "Влада Шарай", "Анастасия Другтейн"]

# 2. B2B СЛОВНИКИ
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

# === ЗАВАНТАЖЕННЯ КОНФІГУ ===
def load_config():
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    except:
        return {
            "SUPPLEMENTS": {"no_discount": 10, "no_description": 10},
            "COSMETICS": {"no_emoji": 10, "no_cross_sell": 10},
            "GENERAL": {"no_question": 5, "stop_word_na_zhal": 15, "gave_up_on_objection": 10}
        }
CONFIG = load_config()

# === PYTHON DETECTORS ===
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
    if "iuse" in text_lower:
        if not ("collagen" in text_lower or "колаген" in text_lower or "коллаген" in text_lower):
            return True
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

# === BITRIX API HELPERS ===
def get_chat_id_via_php(session_id):
    try:
        res = requests.get(PHP_ENDPOINT, params={"session_id": session_id, "key": PHP_SECRET_KEY}, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if 'chat_id' in data: return data['chat_id']
    except: pass
    return None

def find_chat_id_ultimate(lead_id):
    # 1. API Direct
    try:
        res = requests.post(f"{BITRIX_WEBHOOK}imopenlines.crm.chat.get", json={"CRM_ENTITY_TYPE": "LEAD", "CRM_ENTITY": lead_id}).json()
        if res.get('result'): return f"chat{res['result'][0]['CHAT_ID']}"
    except: pass
    
    # 2. Activity + PHP
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

def get_chat_text(lead_id):
    dialog_id = find_chat_id_ultimate(lead_id)
    if not dialog_id: return None
    try:
        res_msg = requests.post(f"{BITRIX_WEBHOOK}im.dialog.messages.get", json={"DIALOG_ID": dialog_id, "LIMIT": 100}).json()
        messages = res_msg.get('result', {}).get('messages', [])
        
        # Исправленная проверка
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
            clean_dialog.append(f"{author_name}: {clean_t}")
            has_text = True
        
        if not has_text: return None
        return "\n".join(clean_dialog)
    except: return None

# === AI LOGIC ===
def generate_prompt(has_emojis, has_question, is_closed_text, is_suppl, has_discount, mode):
    if mode == "B2B":
        return """
Ти — Експерт з комунікацій (РОП). Це діалог B2B (лікар/партнер).
Твоє завдання: Оцінити тон і ввічливість. Оцінку продажів (Score) ставити 0.
JSON: {"product_type": "B2B", "score": 0, "summary": "...", "good_points": "...", "bad_points": "-", "recommendation": "-", "sales_feedback": "..."}
"""
    type_instr = "СИСТЕМА: Це БАДи. Оцінюй як SUPPLEMENTS." if is_suppl else "Визнач категорію (COSMETICS або SUPPLEMENTS)."
    emoji_instr = "СИСТЕМА: Емодзі є." if has_emojis else "СИСТЕМА: Емодзі немає."
    discount_instr = "СИСТЕМА: Знижку знайдено в тексті. Штрафувати заборонено." if has_discount else "СИСТЕМА: Згадок про знижку не знайдено."
    sales_status = "СИСТЕМА: Угода закрита (ТТН). Успіх." if is_closed_text else "СИСТЕМА: Угода НЕ закрита."
    
    question_instr = "СИСТЕМА: Питання немає."
    if has_question: question_instr = "СИСТЕМА: Знак питання є."
    elif is_closed_text: question_instr = "СИСТЕМА: Діалог завершено замовленням. Питання не потрібне."

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

АЛГОРИТМ ОЦІНКИ B2C (100 балів):
1. ВИЗНАЧ СЦЕНАРІЙ: Інтерес (А) або Заперечення (Б).
2. РОЗРАХУНОК ШТРАФІВ:
   🔴 БАДи: Немає знижки -> -{pen_s_disc}. Немає опису -> -{pen_s_desc}.
   🟢 КОСМЕТИКА: Немає емодзі -> -{pen_c_emoji}. Немає cross-sell -> -{pen_c_cross}.
   ⚫ ЗАГАЛЬНІ: Здався на запереченні -> -{pen_g_giveup}. Немає питання в кінці -> -{pen_g_quest}. "На жаль" -> -{pen_g_stop}.

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
  "sales_feedback": "Текст"
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

# === MAIN RUNNER (AUTO-UPDATE) ===
def main():
    print(f"--- GITHUB AUTO-MONITORING (v41) ---")
    
    try:
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open(SHEET_NAME)
        ws_data = sh.worksheet(WORKSHEET_DATA)
        ws_conf = sh.worksheet(WORKSHEET_CONFIG)
        
        # Загружаем существующие ID чтобы избежать дублей
        existing_ids = ws_data.col_values(1) # Колонка A
        print(f"📊 В базі вже є {len(existing_ids)} записів.")
        
    except Exception as e:
        print(f"🔴 Critical Error Google: {e}")
        return

    # 1. Читаємо дату останнього запуску
    last_run_date = ws_conf.acell('B1').value
    if not last_run_date:
        last_run_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"📅 Шукаємо ліди новіші за: {last_run_date}")

    total_processed = 0
    manager_ids_int = [1519, 2077, 6894, 13408]
    
    for mgr_id in manager_ids_int:
        print(f"👤 Менеджер {mgr_id}...", end=" ")
        try:
            payload = {
                "order": {"DATE_CREATE": "ASC"},
                "filter": {"ASSIGNED_BY_ID": mgr_id, ">DATE_CREATE": f"{last_run_date}T00:00:00"},
                "select": ["ID", "TITLE", "STATUS_ID", "DATE_CREATE", "HAS_DEAL", "NAME", "LAST_NAME", "SOURCE_ID"]
            }
            leads = requests.post(f"{BITRIX_WEBHOOK}crm.lead.list", json=payload).json().get('result', [])
            
            if not leads:
                print("Немає нових.")
                continue

            print(f"Знайдено {len(leads)} лідів.")

            for lead in leads:
                source_id = str(lead.get('SOURCE_ID', ''))
                if 'INSTAGRAM' not in source_id.upper(): continue
                
                chat_text = get_chat_text(lead['ID'])
                if not chat_text: continue

                client_name = f"{lead.get('NAME', '')} {lead.get('LAST_NAME', '')}".strip()
                
                # Аналіз
                result = analyze_row(chat_text, client_name)
                
                if result:
                    readable_source = source_id
                    readable_status = lead.get('STATUS_ID')
                    has_deal = "Є" if lead.get('HAS_DEAL') == 'Y' else "Ні"
                    link = f"https://bitrix.emet.in.ua/crm/lead/details/{lead['ID']}/"

                    row_data = [
                        str(lead['ID']), lead['DATE_CREATE'][:10], mgr_id, client_name,
                        readable_source, readable_status, has_deal, link, chat_text[:45000],
                        result.get('product_type'), result.get('score'), result.get('summary'),
                        str(result.get('good_points')), str(result.get('bad_points')),
                        result.get('recommendation'), result.get('sales_feedback')
                    ]
                    
                    # --- ЛОГИКА ОБНОВЛЕНИЯ (v41) ---
                    lead_id_str = str(lead['ID'])
                    if lead_id_str in existing_ids:
                        # Обновляем существующую
                        row_index = existing_ids.index(lead_id_str) + 1
                        # Обновляем диапазон (кроме ID и даты, обновляем статус, сделку и AI данные)
                        # A=1, B=2, C=3... I=9 (Chat), J=10 (Type), K=11 (Score)...
                        # Обновляем все поля строки
                        ws_data.update(range_name=f"A{row_index}:P{row_index}", values=[row_data])
                        print(f"   [♻️ UPD] Лід {lead['ID']} оновлено.")
                    else:
                        # Добавляем новую
                        ws_data.append_row(row_data)
                        existing_ids.append(lead_id_str) # Добавляем в локальный список
                        print(f"   [🆕 NEW] Лід {lead['ID']} додано.")
                    
                    total_processed += 1
                    time.sleep(1.5)
                    
        except Exception as e:
            print(f"Err: {e}")

    # 2. Оновлюємо дату
    today = datetime.now().strftime("%Y-%m-%d")
    ws_conf.update_acell('B1', today)
    print(f"\n✅ [DONE] Оброблено {total_processed} лідів. Дата оновлена на {today}.")

if __name__ == "__main__":
    main()
