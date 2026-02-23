"""
一键同步政策主题概念成分股到 DB3
Usage:
    python -m code.jobs.run_sync_policy_theme
"""
from code.core.db_manager import get_engine, DB3_TABLES
from code.core.logger import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


def _ensure_policy_theme_table():
    """单独建 policy_theme_stocks 表（幂等，已存在则跳过）"""
    ddl = next(
        (d for d in DB3_TABLES if "policy_theme_stocks" in d),
        None,
    )
    if ddl is None:
        logger.warning("未在 DB3_TABLES 中找到 policy_theme_stocks DDL，跳过建表")
        return
    engine = get_engine("db3")
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    logger.info("policy_theme_stocks 表已就绪")


def main():
    logger.info("=" * 55)
    logger.info("  政策主题概念成分股同步")
    logger.info("=" * 55)

    # Step 1: 建表（已存在则跳过）
    logger.info("[Step 1] 确认 DB3 表结构...")
    _ensure_policy_theme_table()

    # Step 2: 拉取 Tushare 概念数据并写库
    logger.info("[Step 2] 同步概念成分股...")
    from code.data.init_aux import task_policy_theme
    task_policy_theme()

    logger.info("=" * 55)
    logger.info("完成！可运行以下命令查看标签体系：")
    logger.info("  python -m code.tools.inspect_policy_themes")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
