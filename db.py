# Тестовое задание

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


def del_elems_from_db(db: dict[str, str],
                      history: dict[int, dict[str, list[str]]],
                      number: int,
                      ) -> dict[str, str]:
    """Удаляет элементы из БД в сценарии с командой UNSET.
    :param db: ДБ, из которой нужно удалить элементы.
    :param history: История удалений элементов внутри транзакций в виде словаря,
    где ключ это номер транзакции, а значение это словарь, в котором ведётся
    лог действий по элементам БД: set и unset.
    :param number: Номер транзакции (уровень вложенности).
    :return: Базу после удаления элементов
    """
    for k in history[number].keys():
        if history[number][k][-1] == 'unset':
            db.pop(k, None)
    return db


def del_elems_from_list_db(list_db: list[dict[str, str]],
                           history: dict[int, dict[str, list[str]]],
                           number: int,
                           ) -> list[dict[str, str]]:
    """Удаляет элементы из списка транзакци в сценарии с командой UNSET.
    :param list_db: Список временных БД до команды UNSET.
    :param history: История удалений элементов внутри транзакций в виде словаря,
    где ключ это номер транзакции, а значение это словарь, в котором ведётся
    лог действий по элементам БД: set и unset.
    :param number: Номер транзакции (уровень вложенности).
    :return: Список транзакций после команды UNSET.
    """
    for db_temp in list_db:
        del_elems_from_db(db=db_temp,
                         history=history,
                         number=number,
                         )
    return list_db


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
    count_transactions: int = 0
    transactions_logs: dict[int, dict[str, list[str]]] = {}

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
                    if count_transactions > 0:
                        (transactions_logs.setdefault(count_transactions, {}).
                         setdefault(enter[1], []).append('set'))
            case 'GET':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде GET <key>')
                else:
                    print(db_temp().get(enter[1], 'NULL'))
            case 'UNSET':
                if words != 2:
                    print('> Повторите ввод и передайте аргумент в виде UNSET <key>')
                else:
                    db_temp().pop(enter[1], None)
                    list_com = transactions_logs.get(count_transactions, {}).get(enter[1], [])
                    if count_transactions > 0 and list_com and list_com[-1] == 'set':
                        (transactions_logs.setdefault(count_transactions, {}).
                         setdefault(enter[1], []).append('unset'))
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
                count_transactions += 1
                deep_transactions.append(deepcopy(db_temp()))
            case 'ROLLBACK':
                if len(deep_transactions) > 0:
                    transactions_logs.popitem()
                    count_transactions -= 1
                    deep_transactions.pop()
                else:
                    print('> Неверная команда')
            case 'COMMIT':
                if len(deep_transactions) > 1:
                    if len(transactions_logs) > 0:
                        del_elems_from_list_db(
                            list_db=deep_transactions,
                            history=transactions_logs,
                            number=count_transactions,
                        )
                        del_elems_from_db(
                            db=db,
                            history=transactions_logs,
                            number=count_transactions,
                        )
                    transactions_logs.popitem()
                    count_transactions -= 1
                    deep_transactions[-2].update(deep_transactions[-1])
                    deep_transactions.pop()
                elif len(deep_transactions) == 1:
                    if len(transactions_logs) > 0:
                        del_elems_from_list_db(
                            list_db=deep_transactions,
                            history=transactions_logs,
                            number=count_transactions,
                        )
                        del_elems_from_db(
                            db=db,
                            history=transactions_logs,
                            number=count_transactions,
                        )
                    transactions_logs.clear()
                    count_transactions -= 1
                    db.update(deep_transactions[-1])
                    deep_transactions.clear()
                else:
                    print('> Неверная команда')
            case 'PRINT':
                print(db, deep_transactions, count_transactions, transactions_logs)
            case 'END':
                break
            case _:
                print('> Неверная команда')


if __name__ == '__main__':
    run_db()
