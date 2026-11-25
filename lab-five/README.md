# Потокобезпечне двійкове дерево пошуку (ЛР5, варіант 18)

Структура С8 (ключі 0..99 999, ленивe видалення), n = 3 потоки: 1 і 3 пишуть, 2 читає. Синхронізація - глобальний
`threading.Lock` або власний RWLock на `threading.Condition`. Звіт: [REPORT.md](REPORT.md).

Потрібен Python 3.14 (стенд - лише стандартна бібліотека; для діаграм - matplotlib).

```sh
python3.14 -m lab5.bench run --lock global          # один прогін на n потоках (D = 10 с, прогрів 1 с)
python3.14 -m lab5.bench run --lock rw
python3.14 -m lab5.bench x1  --lock global          # X_1 і t_cs,w, t_cs,r в одному потоці
python3.14 -m lab5.bench check-readonly             # читання не змінюють полів дерева
python3.14 -m lab5.bench series                     # усе разом, 3 повтори, кожен прогін окремим процесом -> results/
python3.14 -m venv .venv && .venv/bin/pip install matplotlib && .venv/bin/python -m lab5.analyze   # таблиці 1-6 і діаграми
```
