#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List, Dict, Tuple, NewType, Optional
import heapq
import json

from matplotlib.style import available
from quotes import get_all_symbols

Shares = NewType("Shares", Dict[str, float])
NAVs = NewType("NAVs", Dict[str, float])


@dataclass
class Plan:
    from_symbol: str
    to_symbol: str
    from_shares: float
    from_amount: float

    def __str__(self):
        return f"{self.from_symbol} => {self.to_symbol}, shares={self.from_shares:.2f}, amount={self.from_amount:.2f}"


@dataclass
class Record:
    symbol: str
    share: float
    nav: float
    amount: float
    target_amount: float = 0

    def __lt__(self, other):
        return self.symbol < other.symbol


navs: NAVs = {}
small_float = 1e-6


def update_navs():
    get_all_symbols(recent_days=7)
    symbols = open("symbols.txt").read().split()
    date = ""
    for symbol in symbols:
        lines = open(f"symbols/{symbol}.csv").readlines()
        vals = lines[1].split(",")
        if date == "":
            date = vals[1]
        else:
            if date != vals[1]:
                print("old data:", symbol, vals)
        nav = float(vals[2])
        navs[symbol] = nav


def imbalance(shares: Shares) -> float:
    """
    计算组合的不平衡度
    """
    amounts = []
    for symbol, share in shares.items():
        nav = navs[symbol]
        amount = share * nav
        amounts.append(amount)
    total = sum(amounts)
    imba = 0
    for amount in amounts:
        imba += abs(amount / total - 1 / len(amounts))
    return imba


def sub_plan(
    pdict: Dict[str, Record], from_symbols: List[str], to_symbols: List[str]
) -> Tuple[set, set, List[Plan]]:
    """
    产生子计划并修改pdict中剩余的量
    """
    done_from = set()
    done_into = set()
    plans = []

    if (
        (not from_symbols)
        or (not to_symbols)
        or (
            len(from_symbols) == len(to_symbols) == 1
            and from_symbols[0] == to_symbols[0]
        )
    ):
        return done_from, done_into, plans

    print("planning", from_symbols, to_symbols)

    def check_done(record: str, done: set):
        if abs(record.amount - record.target_amount) < small_float:
            done.add(record.symbol)
            return True

    while True:
        to_syms = set(to_symbols) - done_into
        # print(to_syms)
        if len(to_syms) == 0:
            # 没有需要转入的了
            break

        holdings = sorted([(pdict[sym].amount, pdict[sym]) for sym in to_syms])
        amount_into, record_into = holdings[0]
        if check_done(record_into, done_into):
            break

        choices = set(from_symbols) - done_from - set([record_into.symbol])
        if not choices:
            # 没有可转出的选项了
            break

        target_amount = record_into.target_amount
        if amount_into + small_float >= target_amount:
            # 剩余资金最小的也超过要求了，不用转了
            break

        # 需要转多少money进去
        need_amount = target_amount - amount_into

        holdings_choices = sorted(
            [
                (pdict[choice].amount - pdict[choice].target_amount, pdict[choice])
                for choice in choices
            ],
            reverse=True,
        )

        # 取出最大的可转出symbol
        amount_from, record_from = holdings_choices[0]
        if amount_from <= small_float:
            # 榨干了，什么可以转的了
            break

        if amount_from + small_float >= need_amount:
            # 可以把to_symbol的需求量填满
            shares_change = need_amount / record_from.nav
            record_from.amount -= need_amount
            record_from.share -= shares_change
            record_into.amount = target_amount
            record_into.share += need_amount / record_into.nav

            plans.append(
                Plan(
                    from_symbol=record_from.symbol,
                    to_symbol=record_into.symbol,
                    from_shares=shares_change,
                    from_amount=need_amount,
                )
            )
            done_into.add(record_into.symbol)
            done_from |= set([record_into.symbol])
            check_done(record_from, done_from)
        else:
            # 填不满to_symbol的需求量, 但是from_symbol已经转干了
            shares_change = amount_from / record_from.nav
            record_from.amount = record_from.target_amount
            record_from.share -= shares_change
            record_into.amount += amount_from
            record_into.share += amount_from / record_into.nav

            plans.append(
                Plan(
                    from_symbol=record_from.symbol,
                    to_symbol=record_into.symbol,
                    from_shares=shares_change,
                    from_amount=amount_from,
                )
            )
            done_from.add(record_from.symbol)
            done_into |= set([record_from.symbol])
            check_done(record_into, done_into)

        # print('plan: ', plans[-1])

    # print("pdict", pdict)
    # print("dones", done_from, done_into)

    return done_from, done_into, plans


def plan(shares: Shares, target: Optional[List[str]] = None) -> List[Plan]:
    """
    计算再平衡计划
    """
    if not target:
        target = list(shares.keys())

    total_amount = 0
    pdict = {}
    for symbol, share in shares.items():
        nav = navs[symbol]
        amount = share * nav
        total_amount += amount
        pdict[symbol] = Record(share=share, nav=nav, amount=amount, symbol=symbol)

    target_amount = total_amount / len(target)

    # fund company compare
    rows = json.loads(open("funds.data", "rb").read()[22:-162])
    names = dict(row.split(",", 2)[:2] for row in rows)

    todo_source = set(shares.keys())
    todo_target = set(target)
    plans = []

    # 没有持有的, 持有量设为0
    for symbol in todo_target:
        if symbol not in pdict:
            pdict[symbol] = Record(symbol=symbol, share=0, nav=navs[symbol], amount=0)
        pdict[symbol].target_amount = target_amount

    # 1. rebalance between same fund company
    prefixes1 = set(names[symbol][:2] for symbol in todo_source)
    prefixes2 = set(names[symbol][:2] for symbol in todo_target)
    for prefix in prefixes1 & prefixes2:
        symbols1 = []
        symbols2 = []
        for symbol1 in shares:
            if names[symbol1].startswith(prefix):
                symbols1.append(symbol1)
        for symbol2 in target:
            if names[symbol2].startswith(prefix):
                symbols2.append(symbol2)

        done1, done2, sub_plans = sub_plan(pdict, symbols1, symbols2)
        todo_source -= done1
        todo_target -= done2
        plans.extend(sub_plans)

    # 2. rebalance inside portfolio
    symbols = list(todo_source & todo_target)
    done1, done2, sub_plans = sub_plan(pdict, symbols, symbols)
    todo_source -= done1
    todo_target -= done2
    plans.extend(sub_plans)

    # 3. rebalance others
    done1, done2, sub_plans = sub_plan(pdict, list(todo_source), list(todo_target))
    todo_source -= done1
    todo_target -= done2
    plans.extend(sub_plans)

    assert len(todo_source) == 0 and len(todo_target) == 0, [todo_source, todo_target]
    return plans


def main(shares: Shares, target: Optional[List[str]] = None, imba_threshold=5):
    update_navs()

    imba = imbalance(shares) * 100
    print(f"imbalance: {imba:.2f}%")

    if imba > imba_threshold:
        plans = plan(shares, target)
        print("reblanace plan:\n========")
        for pl in plans:
            print(pl)
    else:
        print("no need to rebalance")


if __name__ == "__main__":
    shares: Shares = {
        "002084": 49840.88,
        "001567": 121919.34,
        "002665": 137077.61,
        "519761": 108660.60,
        "001337": 104595.31,
        "001326": 125424.20,
        "002091": 109543.46,
        "005216": 116515.63,
        "005353": 56385.60,
        "001510": 51445.00,
        "004391": 44157.30,
        "003962": 39889.75,
        "004236": 44017.21,
        "002771": 47103.18,
        "001997": 44695.26,
        "004235": 30825.62,
        "001217": 42801.54,
        "003858": 61383.76,
        "519771": 62454.02,
        "003951": 64555.70,
    }
    target = [
        "005050",
        "001301",
        "002658",
        "001217",
        "002084",
        "002451",
        "004454",
        "002079",
        "003858",
        "001425",
        "004569",
        "005353",
        "002462",
        "005216",
        "003806",
        "003851",
        "003951",
        "001523",
        "005178",
        "004235",
    ]
    main(shares, target)
