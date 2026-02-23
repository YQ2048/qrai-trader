"""
QRAI-Trader 数据库管理
负责数据库连接和表结构创建
"""
import atexit

import certifi
from sqlalchemy import create_engine, text
from code.core.config import DB_CONFIGS


# 模块级 Engine 单例缓存（每个 db_key 一个）
_engine_cache: dict = {}


def get_engine(db_key: str):
    """获取指定数据库的 SQLAlchemy Engine（单例，复用连接池）"""
    if db_key in _engine_cache:
        return _engine_cache[db_key]

    cfg = DB_CONFIGS[db_key]
    ssl_ca = certifi.where()
    if 'url' in cfg and cfg['url']:
        url = cfg['url']
    else:
        url = (
            f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
    engine = create_engine(
        url,
        connect_args={'ssl': {'ca': ssl_ca}},
        pool_size=3,
        max_overflow=5,
        pool_recycle=1800,
        pool_pre_ping=True,
    )
    _engine_cache[db_key] = engine
    return engine


def dispose_all_engines():
    """进程退出时统一释放所有连接池"""
    for key, engine in list(_engine_cache.items()):
        try:
            engine.dispose()
        except Exception:
            pass
    _engine_cache.clear()


atexit.register(dispose_all_engines)


# ============================================================
# DB1 表结构：核心行情
# ============================================================
DB1_TABLES = [
    # 交易日历
    """
    CREATE TABLE IF NOT EXISTS trade_cal (
        exchange    VARCHAR(10) NOT NULL,
        cal_date    VARCHAR(8) NOT NULL,
        is_open     INT,
        pretrade_date VARCHAR(8),
        PRIMARY KEY (exchange, cal_date)
    )
    """,
    # 股票基础信息
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        ts_code      VARCHAR(10) PRIMARY KEY,
        symbol       VARCHAR(10),
        name         VARCHAR(20),
        area         VARCHAR(20),
        industry     VARCHAR(20),
        fullname     VARCHAR(100),
        cnspell      VARCHAR(20),
        market       VARCHAR(10),
        exchange     VARCHAR(10),
        list_status  VARCHAR(2),
        list_date    VARCHAR(8),
        delist_date  VARCHAR(8),
        is_hs        VARCHAR(2),
        act_name     VARCHAR(100),
        act_ent_type VARCHAR(100)
    )
    """,
    # 日线行情
    """
    CREATE TABLE IF NOT EXISTS stock_daily (
        ts_code    VARCHAR(10) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        open       DOUBLE,
        high       DOUBLE,
        low        DOUBLE,
        close      DOUBLE,
        pre_close  DOUBLE,
        `change`   DOUBLE,
        pct_chg    DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 复权因子
    """
    CREATE TABLE IF NOT EXISTS adj_factor (
        ts_code    VARCHAR(10) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        adj_factor DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 每日指标
    """
    CREATE TABLE IF NOT EXISTS daily_basic (
        ts_code         VARCHAR(10) NOT NULL,
        trade_date      VARCHAR(8) NOT NULL,
        close           DOUBLE,
        turnover_rate   DOUBLE,
        turnover_rate_f DOUBLE,
        volume_ratio    DOUBLE,
        pe              DOUBLE,
        pe_ttm          DOUBLE,
        pb              DOUBLE,
        ps              DOUBLE,
        ps_ttm          DOUBLE,
        dv_ratio        DOUBLE,
        dv_ttm          DOUBLE,
        total_share     DOUBLE,
        float_share     DOUBLE,
        free_share      DOUBLE,
        total_mv        DOUBLE,
        circ_mv         DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 指数日线行情
    """
    CREATE TABLE IF NOT EXISTS index_daily (
        ts_code    VARCHAR(20) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        close      DOUBLE,
        open       DOUBLE,
        high       DOUBLE,
        low        DOUBLE,
        pre_close  DOUBLE,
        `change`   DOUBLE,
        pct_chg    DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 大盘指数每日指标
    """
    CREATE TABLE IF NOT EXISTS index_dailybasic (
        ts_code         VARCHAR(20) NOT NULL,
        trade_date      VARCHAR(8) NOT NULL,
        total_mv        DOUBLE,
        float_mv        DOUBLE,
        total_share     DOUBLE,
        float_share     DOUBLE,
        free_share      DOUBLE,
        turnover_rate   DOUBLE,
        turnover_rate_f DOUBLE,
        pe              DOUBLE,
        pe_ttm          DOUBLE,
        pb              DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
]

# ============================================================
# DB2 表结构：资金与筹码
# ============================================================
DB2_TABLES = [
    # 个股资金流向
    """
    CREATE TABLE IF NOT EXISTS moneyflow (
        ts_code         VARCHAR(10) NOT NULL,
        trade_date      VARCHAR(8) NOT NULL,
        buy_sm_vol      DOUBLE,
        buy_sm_amount   DOUBLE,
        sell_sm_vol     DOUBLE,
        sell_sm_amount  DOUBLE,
        buy_md_vol      DOUBLE,
        buy_md_amount   DOUBLE,
        sell_md_vol     DOUBLE,
        sell_md_amount  DOUBLE,
        buy_lg_vol      DOUBLE,
        buy_lg_amount   DOUBLE,
        sell_lg_vol     DOUBLE,
        sell_lg_amount  DOUBLE,
        buy_elg_vol     DOUBLE,
        buy_elg_amount  DOUBLE,
        sell_elg_vol    DOUBLE,
        sell_elg_amount DOUBLE,
        net_mf_vol      DOUBLE,
        net_mf_amount   DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 每日筹码及胜率
    """
    CREATE TABLE IF NOT EXISTS cyq_perf (
        ts_code     VARCHAR(10) NOT NULL,
        trade_date  VARCHAR(8) NOT NULL,
        his_low     DOUBLE,
        his_high    DOUBLE,
        cost_5pct   DOUBLE,
        cost_15pct  DOUBLE,
        cost_50pct  DOUBLE,
        cost_85pct  DOUBLE,
        cost_95pct  DOUBLE,
        weight_avg  DOUBLE,
        winner_rate DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 沪深港通资金流向
    """
    CREATE TABLE IF NOT EXISTS moneyflow_hsgt (
        trade_date  VARCHAR(8) PRIMARY KEY,
        ggt_ss      DOUBLE,
        ggt_sz      DOUBLE,
        hgt         DOUBLE,
        sgt         DOUBLE,
        north_money DOUBLE,
        south_money DOUBLE
    )
    """,
    # 融资融券交易汇总
    """
    CREATE TABLE IF NOT EXISTS margin (
        trade_date     VARCHAR(8) NOT NULL,
        exchange_id    VARCHAR(10) NOT NULL,
        rzye           DOUBLE,
        rzmre          DOUBLE,
        rzche          DOUBLE,
        rqye           DOUBLE,
        rqmcl          DOUBLE,
        rzrqye         DOUBLE,
        rqyl           DOUBLE,
        PRIMARY KEY (trade_date, exchange_id)
    )
    """,
    # 融资融券交易明细
    """
    CREATE TABLE IF NOT EXISTS margin_detail (
        trade_date  VARCHAR(8) NOT NULL,
        ts_code     VARCHAR(10) NOT NULL,
        name        VARCHAR(20),
        rzye        DOUBLE,
        rqye        DOUBLE,
        rzmre       DOUBLE,
        rqyl        DOUBLE,
        rzche       DOUBLE,
        rqchl       DOUBLE,
        rqmcl       DOUBLE,
        rzrqye      DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 沪深股通十大成交股
    """
    CREATE TABLE IF NOT EXISTS hsgt_top10 (
        trade_date  VARCHAR(8) NOT NULL,
        ts_code     VARCHAR(10) NOT NULL,
        name        VARCHAR(20),
        close       DOUBLE,
        `change`    DOUBLE,
        `rank`      INT,
        market_type VARCHAR(2),
        amount      DOUBLE,
        net_amount  DOUBLE,
        buy         DOUBLE,
        sell        DOUBLE,
        PRIMARY KEY (ts_code, trade_date, market_type),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 指数成分和权重（月度）
    """
    CREATE TABLE IF NOT EXISTS index_weight (
        index_code  VARCHAR(20) NOT NULL,
        con_code    VARCHAR(10) NOT NULL,
        trade_date  VARCHAR(8) NOT NULL,
        weight      DOUBLE,
        PRIMARY KEY (index_code, con_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
]

# ============================================================
# DB3 表结构：策略与辅助
# ============================================================
DB3_TABLES = [
    # ST股票列表
    """
    CREATE TABLE IF NOT EXISTS stock_st (
        ts_code    VARCHAR(10) NOT NULL,
        name       VARCHAR(20),
        trade_date VARCHAR(8) NOT NULL,
        type       VARCHAR(10),
        type_name  VARCHAR(20),
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 每日涨跌停价格
    """
    CREATE TABLE IF NOT EXISTS stk_limit (
        ts_code    VARCHAR(10) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        pre_close  DOUBLE,
        up_limit   DOUBLE,
        down_limit DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 涨跌停和炸板数据
    """
    CREATE TABLE IF NOT EXISTS limit_list_d (
        trade_date     VARCHAR(8) NOT NULL,
        ts_code        VARCHAR(10) NOT NULL,
        industry       VARCHAR(30),
        name           VARCHAR(20),
        close          DOUBLE,
        pct_chg        DOUBLE,
        amount         DOUBLE,
        limit_amount   DOUBLE,
        float_mv       DOUBLE,
        total_mv       DOUBLE,
        turnover_ratio DOUBLE,
        fd_amount      DOUBLE,
        first_time     VARCHAR(20),
        last_time      VARCHAR(20),
        open_times     INT,
        up_stat        VARCHAR(20),
        limit_times    INT,
        `limit`        VARCHAR(2),
        PRIMARY KEY (ts_code, trade_date, `limit`),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 财报披露计划
    """
    CREATE TABLE IF NOT EXISTS disclosure_date (
        ts_code     VARCHAR(10) NOT NULL,
        end_date    VARCHAR(8) NOT NULL,
        ann_date    VARCHAR(8),
        pre_date    VARCHAR(8),
        actual_date VARCHAR(8),
        modify_date VARCHAR(20),
        PRIMARY KEY (ts_code, end_date)
    )
    """,
    # 业绩预告
    """
    CREATE TABLE IF NOT EXISTS forecast (
        ts_code         VARCHAR(10) NOT NULL,
        ann_date        VARCHAR(8),
        end_date        VARCHAR(8) NOT NULL,
        type            VARCHAR(10),
        p_change_min    DOUBLE,
        p_change_max    DOUBLE,
        net_profit_min  DOUBLE,
        net_profit_max  DOUBLE,
        last_parent_net DOUBLE,
        first_ann_date  VARCHAR(8),
        summary         TEXT,
        change_reason   TEXT,
        PRIMARY KEY (ts_code, end_date)
    )
    """,
    # 业绩快报
    """
    CREATE TABLE IF NOT EXISTS express (
        ts_code                    VARCHAR(10) NOT NULL,
        ann_date                   VARCHAR(8),
        end_date                   VARCHAR(8) NOT NULL,
        revenue                    DOUBLE,
        operate_profit             DOUBLE,
        total_profit               DOUBLE,
        n_income                   DOUBLE,
        total_assets               DOUBLE,
        total_hldr_eqy_exc_min_int DOUBLE,
        diluted_eps                DOUBLE,
        diluted_roe                DOUBLE,
        yoy_net_profit             DOUBLE,
        bps                        DOUBLE,
        yoy_sales                  DOUBLE,
        yoy_op                     DOUBLE,
        yoy_tp                     DOUBLE,
        yoy_dedu_np                DOUBLE,
        yoy_eps                    DOUBLE,
        yoy_roe                    DOUBLE,
        growth_assets              DOUBLE,
        yoy_equity                 DOUBLE,
        growth_bps                 DOUBLE,
        or_last_year               DOUBLE,
        op_last_year               DOUBLE,
        tp_last_year               DOUBLE,
        np_last_year               DOUBLE,
        eps_last_year              DOUBLE,
        open_net_assets            DOUBLE,
        open_bps                   DOUBLE,
        perf_summary               TEXT,
        is_audit                   INT,
        remark                     TEXT,
        PRIMARY KEY (ts_code, end_date)
    )
    """,
    # 开盘集合竞价数据
    """
    CREATE TABLE IF NOT EXISTS stk_auction (
        ts_code    VARCHAR(10) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        close      DOUBLE,
        open       DOUBLE,
        high       DOUBLE,
        low        DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        vwap       DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 龙虎榜每日明细
    """
    CREATE TABLE IF NOT EXISTS top_list (
        trade_date    VARCHAR(8) NOT NULL,
        ts_code       VARCHAR(10) NOT NULL,
        name          VARCHAR(20),
        close         DOUBLE,
        pct_change    DOUBLE,
        turnover_rate DOUBLE,
        amount        DOUBLE,
        l_sell        DOUBLE,
        l_buy         DOUBLE,
        l_amount      DOUBLE,
        net_amount    DOUBLE,
        net_rate      DOUBLE,
        amount_rate   DOUBLE,
        float_values  DOUBLE,
        reason        VARCHAR(100),
        PRIMARY KEY (ts_code, trade_date, reason(50)),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 龙虎榜机构明细
    """
    CREATE TABLE IF NOT EXISTS top_inst (
        trade_date VARCHAR(8) NOT NULL,
        ts_code    VARCHAR(10) NOT NULL,
        exalter    VARCHAR(200),
        side       VARCHAR(2),
        buy        DOUBLE,
        buy_rate   DOUBLE,
        sell       DOUBLE,
        sell_rate  DOUBLE,
        net_buy    DOUBLE,
        reason     VARCHAR(100),
        KEY idx_date_code (trade_date, ts_code)
    )
    """,
    # 申万行业日线行情
    """
    CREATE TABLE IF NOT EXISTS sw_daily (
        ts_code    VARCHAR(20) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        name       VARCHAR(30),
        open       DOUBLE,
        low        DOUBLE,
        high       DOUBLE,
        close      DOUBLE,
        `change`   DOUBLE,
        pct_change DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        pe         DOUBLE,
        pb         DOUBLE,
        float_mv   DOUBLE,
        total_mv   DOUBLE,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_trade_date (trade_date)
    )
    """,
    # 股东增减持
    """
    CREATE TABLE IF NOT EXISTS stk_holdertrade (
        ts_code      VARCHAR(10) NOT NULL,
        ann_date     VARCHAR(8) NOT NULL,
        holder_name  VARCHAR(100),
        holder_type  VARCHAR(2),
        in_de        VARCHAR(2),
        change_vol   DOUBLE,
        change_ratio DOUBLE,
        after_share  DOUBLE,
        after_ratio  DOUBLE,
        avg_price    DOUBLE,
        total_share  DOUBLE,
        begin_date   VARCHAR(8),
        close_date   VARCHAR(8),
        KEY idx_code_date (ts_code, ann_date)
    )
    """,
    # 大宗交易
    """
    CREATE TABLE IF NOT EXISTS block_trade (
        ts_code    VARCHAR(10) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        price      DOUBLE,
        vol        DOUBLE,
        amount     DOUBLE,
        buyer      VARCHAR(200),
        seller     VARCHAR(200),
        KEY idx_date (trade_date),
        KEY idx_code (ts_code)
    )
    """,
    # 限售股解禁
    """
    CREATE TABLE IF NOT EXISTS share_float (
        ts_code     VARCHAR(10) NOT NULL,
        ann_date    VARCHAR(8),
        float_date  VARCHAR(8) NOT NULL,
        float_share DOUBLE,
        float_ratio DOUBLE,
        holder_name VARCHAR(100),
        share_type  VARCHAR(30),
        KEY idx_code_date (ts_code, float_date),
        KEY idx_float_date (float_date)
    )
    """,
    # 股东人数
    """
    CREATE TABLE IF NOT EXISTS stk_holdernumber (
        ts_code    VARCHAR(10) NOT NULL,
        ann_date   VARCHAR(8) NOT NULL,
        end_date   VARCHAR(8) NOT NULL,
        holder_num INT,
        PRIMARY KEY (ts_code, end_date),
        KEY idx_ann_date (ann_date)
    )
    """,
    # 财务指标数据
    """
    CREATE TABLE IF NOT EXISTS fina_indicator (
        ts_code              VARCHAR(10) NOT NULL,
        ann_date             VARCHAR(8),
        end_date             VARCHAR(8) NOT NULL,
        eps                  DOUBLE,
        dt_eps               DOUBLE,
        total_revenue_ps     DOUBLE,
        revenue_ps           DOUBLE,
        capital_rese_ps      DOUBLE,
        surplus_rese_ps      DOUBLE,
        undist_profit_ps     DOUBLE,
        extra_item           DOUBLE,
        profit_dedt          DOUBLE,
        gross_margin         DOUBLE,
        current_ratio        DOUBLE,
        quick_ratio          DOUBLE,
        cash_ratio           DOUBLE,
        ar_turn              DOUBLE,
        ca_turn              DOUBLE,
        fa_turn              DOUBLE,
        assets_turn          DOUBLE,
        op_income            DOUBLE,
        ebit                 DOUBLE,
        ebitda               DOUBLE,
        fcff                 DOUBLE,
        fcfe                 DOUBLE,
        netdebt              DOUBLE,
        tangible_asset       DOUBLE,
        working_capital      DOUBLE,
        networking_capital   DOUBLE,
        invest_capital       DOUBLE,
        retained_earnings    DOUBLE,
        bps                  DOUBLE,
        ocfps                DOUBLE,
        retainedps           DOUBLE,
        cfps                 DOUBLE,
        ebit_ps              DOUBLE,
        fcff_ps              DOUBLE,
        fcfe_ps              DOUBLE,
        netprofit_margin     DOUBLE,
        grossprofit_margin   DOUBLE,
        roe                  DOUBLE,
        roe_waa              DOUBLE,
        roe_dt               DOUBLE,
        roa                  DOUBLE,
        npta                 DOUBLE,
        roic                 DOUBLE,
        debt_to_assets       DOUBLE,
        assets_to_eqt        DOUBLE,
        netprofit_yoy        DOUBLE,
        dt_netprofit_yoy     DOUBLE,
        or_yoy               DOUBLE,
        q_sales_yoy          DOUBLE,
        q_op_qoq             DOUBLE,
        equity_yoy           DOUBLE,
        rd_exp               DOUBLE,
        update_flag          VARCHAR(2),
        PRIMARY KEY (ts_code, end_date),
        KEY idx_ann_date (ann_date)
    )
    """,
    # 申万行业分类
    """
    CREATE TABLE IF NOT EXISTS index_classify (
        index_code  VARCHAR(20) NOT NULL,
        industry_name VARCHAR(30),
        level       VARCHAR(5),
        industry_code VARCHAR(10),
        is_pub      VARCHAR(2),
        parent_code VARCHAR(10),
        PRIMARY KEY (index_code)
    )
    """,
    # 申万行业成分构成
    """
    CREATE TABLE IF NOT EXISTS index_member_all (
        l1_code  VARCHAR(20),
        l1_name  VARCHAR(30),
        l2_code  VARCHAR(20),
        l2_name  VARCHAR(30),
        l3_code  VARCHAR(20),
        l3_name  VARCHAR(30),
        ts_code  VARCHAR(10) NOT NULL,
        name     VARCHAR(20),
        in_date  VARCHAR(8),
        out_date VARCHAR(8),
        is_new   VARCHAR(2),
        KEY idx_ts_code (ts_code),
        KEY idx_l1 (l1_code),
        KEY idx_l3 (l3_code)
    )
    """,
    # 十五五政策主题概念成分股（月频同步，替代行业关键词匹配）
    """
    CREATE TABLE IF NOT EXISTS policy_theme_stocks (
        ts_code      VARCHAR(10)  NOT NULL,
        name         VARCHAR(50),
        concept_id   VARCHAR(20)  NOT NULL,
        concept_name VARCHAR(100),
        updated_date VARCHAR(8),
        PRIMARY KEY (ts_code, concept_id),
        KEY idx_ts_code (ts_code),
        KEY idx_concept (concept_id)
    )
    """,
    # 策略信号表（系统计算结果）
    """
    CREATE TABLE IF NOT EXISTS strategy_signals (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        signal_date    VARCHAR(8) NOT NULL,
        ts_code        VARCHAR(10) NOT NULL,
        strategy_type  VARCHAR(20),
        score          DOUBLE,
        vlm_score      INT,
        vlm_reason     TEXT,
        auction_status VARCHAR(20),
        suggest_buy_low  DOUBLE,
        suggest_buy_high DOUBLE,
        skyline_stop   DOUBLE,
        status         VARCHAR(20) DEFAULT 'PENDING',
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_signal (signal_date, ts_code),
        KEY idx_status (status)
    )
    """,
]


def create_all_tables():
    """在三个数据库中分别创建对应的表"""
    table_map = {
        'db1': ('核心行情', DB1_TABLES),
        'db2': ('资金与筹码', DB2_TABLES),
        'db3': ('策略与辅助', DB3_TABLES),
    }

    for db_key, (desc, tables) in table_map.items():
        print(f"\n{'='*50}")
        print(f"正在创建 {db_key} ({desc}) 的表结构...")
        print(f"{'='*50}")
        engine = get_engine(db_key)
        with engine.connect() as conn:
            for ddl in tables:
                # 提取表名用于日志
                table_name = ddl.split('EXISTS')[1].split('(')[0].strip()
                try:
                    conn.execute(text(ddl))
                    conn.commit()
                    print(f"  ✓ {table_name}")
                except Exception as e:
                    print(f"  ✗ {table_name}: {e}")
        engine.dispose()

    print(f"\n{'='*50}")
    print("所有表创建完成！")


if __name__ == '__main__':
    create_all_tables()
