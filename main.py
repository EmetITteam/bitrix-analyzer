import gspread
import json
import time
import re
import os
from openai import OpenAI

# --- НАЛАШТУВАННЯ ---
OPENAI_API_KEY = "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" 
CREDENTIALS_FILE = "creds.json"
CONFIG_FILE = "config.json"
SHEET_NAME = "BitrixChat"
WORKSHEET_NAME = "Final_V21_27_1006" 
AI_MODEL = "gpt-4o" 
# --------------------

client = OpenAI(api_key=OPENAI_API_KEY)

# 1. ЖОРСТКИЙ СПИСОК МЕНЕДЖЕРІВ
MANAGER_NAMES = [
    "Яна Наконечна", "Софія Кривенко", "Влада Шарай", "Анастасия Другтейн"
]

# 2. B2B СЛОВНИКИ (Safe Mode)
# Видалено "кабінет", "реєстрація" щоб не плутати з сайтом.
B2B_KEYWORDS = [
    "розклад", "расписание", "семінар", "семинар", "навчання", "обучение",
    "прайс косметолога", "прайс для косметологов", "я косметолог", "я врач", "я лікар",
    "диплом", "сертифікат", "сертификат", 
    "співпрац", "сотруднич", "опт", "гурт",
    "протокол", "protocol", "анкета", # Протоколи залишаємо, це зазвичай лікарі
    "neuramis", "нейраміс", "medytox", "медитокс", "neuronox", "нейронокс",
    "блогер", "blogger", "бартер", "barter", "реклам"
]

B2B_NAMES = ["dr", "dr.", "лікар", "врач", "косметолог", "dermatolog", "cosmetolog", "clinic", "клініка", "клиника", "md", "estet"]

# 3. REFERRAL
REFERRAL_KEYWORDS = ["порадьте косметолога", "посоветуйте", "де зробити", "контакти лікаря", "записатись на процедуру", "уколоть"]

# 4. ФАКТИ
CLOSE_WORDS = ["ттн", "накладна", "номер накладної", "дякуємо за замовлення", "оформлено", "реквізити", "оплату отримали"]
BRAND_EMOJIS = ["🌿", "🍃", "☘️", "🌱", "🍀", "💰", "✨", "💫", "🛒", "🛍", "💚", "🤍", "💧", "☺️", "🙌🏻", "🥰", "💌"]
DISCOUNT_WORDS = ["знижка", "скидка", "парна", "парная", "від 2", "от 2", "набір", "набор", "курс", "15%", "-%"]

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
    tail_clean = re.sub(r'[^\w\s\?\.!]', '', tail) 
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

# === ГЕНЕРАТОР ПРОМПТУ ===
def generate_prompt(has_emojis, has_question, is_closed_text, is_suppl, has_discount, mode):
    
    # B2B
    if mode == "B2B":
        return """
Ти — Експерт з комунікацій. Це діалог B2B (лікар/партнер).
Твоє завдання: Оцінити тон і ввічливість. Оцінку продажів (Score) ставити 0.
JSON: {"product_type": "B2B", "score": 0, "summary": "...", "good_points": "...", "bad_points": "-", "recommendation": "-", "sales_feedback": "..."}
"""

    # B2C
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

АЛГОРИТМ ОЦІНКИ B2C (Початково 100 балів):

1. ВИЗНАЧ СЦЕНАРІЙ ДІАЛОГУ:
   - Сценарій А (Інтерес): Клієнт запитує ціну, погоджується або мовчить.
   - Сценарій Б (Заперечення): Клієнт пише "Ні", "Дорого", "Подумаю".

2. РОЗРАХУНОК ШТРАФІВ:

   🔴 БАДи (SUPPLEMENTS):
   - ЗНИЖКА: Дивись ФАКТ №5. 
     - Якщо система каже, що знижка є -> ОК.
     - Якщо немає -> Мінус {pen_s_disc}. (Bad: "Не запропоновано вигоду від кількості").
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
    
    # 1. B2B ФІЛЬТР
    is_b2b_python = check_is_b2b_python(dialog_text, client_name)
    if is_b2b_python:
        mode = "B2B"
    else:
        if check_keywords(dialog_text, REFERRAL_KEYWORDS):
            return {
                "product_type": "B2C_REFERRAL", "score": 0, "summary": "Пошук косметолога.",
                "good_points": "-", "bad_points": "-", "recommendation": "-", "sales_feedback": "Технічний запит"
            }
        
        has_manager = check_manager_presence(dialog_text)
        if not has_manager:
            return {
                "product_type": "NO_REPLY", "score": 0, "summary": "Без відповіді",
                "good_points": "-", "bad_points": "Ігнорування", "recommendation": "Відповісти", "sales_feedback": "Втрачений лід"
            }
        mode = "B2C"

    # 2. АНАЛІЗ
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

def main():
    print(f"--- ЗАПУСК АНАЛІЗАТОРА V38 (STABLE) ---")
    
    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        sh = gc.open(SHEET_NAME)
        ws = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"Critical Error: {e}")
        return

    headers = ["Тип (AI)", "Оцінка", "Резюме", "Плюси", "Мінуси", "Рекомендація", "Коментар РОП"]
    ws.update(range_name="J1:P1", values=[headers])
    
    print("Завантаження даних (щоб не блокував Google)...")
    all_rows = ws.get_all_values()
    total = len(all_rows)
    
    for i in range(1, total):
        row_num = i + 1
        row = all_rows[i]
        
        if len(row) <= 8: continue
        
        text = row[8]
        client_name = row[3] if len(row) > 3 else ""
        
        # Перевірка вже існуючої оцінки в пам'яті
        existing_status = row[9] if len(row) > 9 else ""
        if existing_status and len(str(existing_status)) > 1:
            continue

        print(f"[{i}/{total-1}] Рядок {row_num}...", end=" ")
        
        result = analyze_row(text, client_name)
        
        if result:
            data = [
                result.get('product_type', '-'),
                result.get('score', '-'),
                result.get('summary', '-'),
                str(result.get('good_points', '-')),
                str(result.get('bad_points', '-')),
                result.get('recommendation', '-'),
                result.get('sales_feedback', '-')
            ]
            try:
                # Пауза 1.5 секунди - гарантія від бану
                time.sleep(1.5) 
                ws.update(range_name=f"J{row_num}:P{row_num}", values=[data])
                print(f"OK! -> {result.get('product_type')} ({result.get('score')})")
            except Exception as e:
                print(f"Write Error: {e}")
                time.sleep(10)
        else:
            print("SKIP (Error/Empty)")
            try: ws.update(range_name=f"J{row_num}", values=[["ERROR"]])
            except: pass

    print("\n[DONE] Робота завершена!")

if __name__ == "__main__":
    main()
