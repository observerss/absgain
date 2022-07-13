import pandas as pd
import numpy as np
import pickle
import random
import logging
import multiprocessing
import sys
assert sys.platform == 'linux'

def sample(corr, ret, k):
    idxs = random.sample(range(len(corr)), k)
    subcorr = corr[idxs][:, idxs]
    mean_subcorr = subcorr[np.tril(subcorr, -1).nonzero()].mean()
    if len(ret.shape) == 1:
        # no rebalance
        mean_ret = ret[idxs].mean()
    else:
        # with rebalance
        last_idx = ret[idxs, :].shape[1] - 1
        total = 1
        shares = total / len(idxs) / ret[idxs, last_idx]
        while last_idx > 0:
            last_idx = max(0, last_idx - 126)
            total = (ret[idxs, last_idx] * shares).sum()
            shares = total / len(idxs) / ret[idxs, last_idx]
        mean_ret = total
    return [mean_ret, mean_subcorr, idxs]


def sharpe(arr, risk_free_return=0.00):
    daily_risk_free_return = risk_free_return / 252.
    daily_return = arr[:, :-1] / arr[:, 1:]  - 1
    excess_daily_return = daily_return - daily_risk_free_return
    return excess_daily_return.mean(axis=1) / excess_daily_return.std(axis=1) * np.sqrt(252)

def best(k=10, use_selected=False, remove_recent_year=False, rebalanced=False):
    dfs = pickle.load(open('dfs.pkl', 'rb'))
    arr = []
    size = 0
    selected = open('selected.txt', 'r').read().split()
    all_symbols = []
    for symbol, df in dfs.items():
        if use_selected and symbol not in selected:
            continue
        if df is not None:
            all_symbols.append(symbol)
            arr.append(df.累计净值.values)
            if size == 0 or size > arr[-1].shape[0]:
                size = arr[-1].shape[0]
    arr = np.stack(list(map(lambda x: x[:size], arr)))
    all_symbols = np.array(all_symbols)
    s = sharpe(arr)
    arr = arr[s > 1.4]
    all_symbols = all_symbols[s > 1.4]
    if remove_recent_year:
        arr = arr[:, 252:]
    print(arr.shape)
    pickle.dump(arr, open('arr.pkl', 'wb'))

    arr = np.divide(arr, arr[:, -1][:, None])  # normalize
    logr = np.log(arr[:, :-1] / arr[:, 1:])  # log of return
    corr = np.corrcoef(logr)
    ret = arr[:, 0] if rebalanced is False else arr
    result_queue = multiprocessing.Queue()

    # executor = ProcessPoolExecutor(8)
    # for _ in range(8):
    #     executor.submit(best_worker, corr, ret, all_symbols, k, result_queue)
    for _ in range(multiprocessing.cpu_count()):
        multiprocessing.Process(target=best_worker, args=(corr, ret, all_symbols, k, result_queue, arr.shape[1]), daemon=True).start()
    best_score = 0
    best_ret = 0
    best_corr = 0
    best_idxs = []
    best_symbols = []
    while True:
        score, ret, corr, idxs, symbols = result_queue.get()
        sys.stdout.write('.')
        sys.stdout.flush()
        if best_score == 0 or best_score < score:
            best_score = score
            best_ret = ret
            best_corr = corr
            best_idxs = idxs
            best_symbols = symbols
            print()
            print(best_score, best_ret, best_corr, best_idxs, best_symbols)
            with open(f'mix_k{k}_{"selected" if use_selected else "all"}_till{"1yago" if remove_recent_year else "now"}{"_rebalanced" if rebalanced else ""}.txt', 'a') as f:
                f.write(f'{best_score}, {best_ret}, {best_corr}, {best_idxs}, {best_symbols.tolist()}\n')


def test_worker(*args, **kwargs):
    logging.error("aha", args, kwargs)


def best_worker(corr, ret, symbols, k, q, length):
    try:
        best_score = 0
        best_ret = 0
        best_corr = 0
        best_idxs = []
        best_symbols = []
        count = 0
        while True:
            s_ret, s_corr, s_idxs = sample(corr, ret, k)
            count += 1
            score = s_ret ** (252. / length) + (1 - s_corr) / 5
            if best_score == 0 or best_score < score:
                best_score = score
                best_ret = s_ret
                best_corr = s_corr
                best_idxs = s_idxs
                best_symbols = symbols[s_idxs]
            if count % 10000 == 0:
                q.put((best_score, best_ret, best_corr, best_idxs, best_symbols))
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception('')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # best(k=10, use_selected=False, remove_recent_year=True)
    # best(k=10, use_selected=True, remove_recent_year=True)
    best(k=20, use_selected=False, remove_recent_year=False, rebalanced=True)
    # best(k=20, use_selected=False, remove_recent_year=False, rebalanced=False)
    # best(k=20, use_selected=False, remove_recent_year=True, rebalanced=True)
    # best(k=10, use_selected=True, remove_recent_year=False)
