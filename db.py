from collections import Counter
from copy import deepcopy

deep_transactions: list[dict[str, str]] = []
db: dict[str, str] = {}


def db_temp() -> dict[str, str]:
    """Функция возвращает рабочую или временную БД в зависимости от
    контекста. Если находимся в транзакции, то возвращаем временную БД,
    иначе - рабочую.

    :return:
        dict[str, str]: Последний слой транзакции, если она активна,
                        иначе основная база данных.
    """
    return deep_transactions[-1] if len(deep_transactions) > 0 else db


def run_db() -> None:
    """Запускает основной цикл базы данных с поддержкой команд.

    Поддерживаются команды:
        SET <key> <value>      — сохранить значение по ключу
        GET <key>              — получить значение по ключу
        UNSET <key>            — удалить значение по ключу
        COUNTS <value>         — сколько раз значение встречается
        FIND <value>           — найти все ключи с заданным значением
        BEGIN                  — начать новую транзакцию
        ROLLBACK               — откатить текущую транзакцию
        COMMIT                 — зафиксировать изменения
        END                    — завершить приложение
    """
    while True:

        try:
            line = input('> ')
        except EOFError:
            print('EOFError')
            break

        enter = line.strip().split()
        if not enter:
            continue

        command = enter[0].upper()
        words = len(enter)

        match command:
            case 'SET':
                if words != 3:
                    print('> Повторите ввод и передайте аргумент в виде SET <key> <value>')
                else:
                    db_temp()[enter[1]] = enter[2]
            case 'GET':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде GET <key>')
                else:
                    print(db_temp().get(enter[1], 'NULL'))
            case 'UNSET':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде UNSET <key>')
                else:
                    db_temp().pop(enter[1])
            case 'COUNTS':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде UNSET <value>')
                else:
                    count = Counter(db_temp().values())
                    print(count[enter[1]])
            case 'FIND':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде FIND <value>')
                else:
                    find_keys = [key for key, value in db_temp().items() if value == enter[1]]
                    print(', '.join(find_keys))
            case 'BEGIN':
                deep_transactions.append(deepcopy(db_temp()))
            case 'ROLLBACK':
                if len(deep_transactions) > 0:
                    deep_transactions.pop()
                else:
                    print('> Неверная команда')
            case 'COMMIT':
                if len(deep_transactions) > 1:
                    deep_transactions[-2].update(deep_transactions[-1])
                    deep_transactions.pop()
                elif len(deep_transactions) == 1:
                    db.update(deep_transactions[-1])
                    deep_transactions.clear()
                else:
                    print('> Неверная команда')
            case 'PRINT':
                print(db, deep_transactions)
            case 'END':
                break
            case _:
                print('> Неверная команда')


if __name__ == '__main__':
    run_db()
