# 操作实践

## 每半年更新数据并重新选基

1. 访问 `quotes.py` 中的[全基金连接](http://fund.eastmoney.com/data/fundranking.html#thh;c0;r;sdm;pn10000;dasc;qsd20200924;qed20210924;qdii;zq;gg;gzbd;gzfs;bbzt;sfbb), 打开inspect窗口, 更新`funds.data`的数据
2. 执行 `quotes.py` 的以下三个函数
    
    ```bash
    select_symbols()
    get_all_symbols()
    make_dfs(min_hist=800, min_size=3)  # 最少800天交易, 最小3亿规模
    ```

3. 执行 `portfolio.py` 的指定函数选基, 选基最好执行1个小时以上, 将会随机尝试各个组合并输出当前最佳组合

    ```bash
    best(k=20, use_selected=False, remove_recent_year=False, rebalanced=True)
    ```

4. 用 `rebalance.py` 计算换仓公式, 修改代码中的 `shares` 和 `targets`, 自动计算换仓方式, 注意 `shares` 应填入份额而不是金额

    ```bash
    main(shares, targets)
    ```
