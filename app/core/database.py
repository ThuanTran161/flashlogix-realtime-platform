from typing import Generator
from fastapi import HTTPException
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from app.core.config import settings

# Engine configuration with high-concurrency connection pooling
engine = create_engine(
    settings.get_database_url(),
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,
    echo=settings.DEBUG and settings.APP_ENV == "development",
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

Base = declarative_base()


def ensure_oltp_schema_and_constraints() -> None:
    """Creates OLTP tables (including Outbox) and idempotently enforces CHECK constraints."""
    import app.models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    constraints = [
        ("products", "ck_products_stock_non_negative", "CHECK (stock >= 0)"),
        ("products", "ck_products_price_positive", "CHECK (price > 0)"),
        ("orders", "ck_orders_quantity_positive", "CHECK (quantity > 0)"),
        ("orders", "ck_orders_total_non_negative", "CHECK (total_amount >= 0)"),
    ]
    with engine.begin() as conn:
        for table, cname, expr in constraints:
            conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = '{cname}'
                        ) THEN
                            ALTER TABLE {table} ADD CONSTRAINT {cname} {expr};
                        END IF;
                    END $$;
                    """
                )
            )


def get_db() -> Generator[Session, None, None]:
    """FastAPI Dependency Injection provider for SQLAlchemy sessions.

    Guarantees that database sessions are always cleanly closed in the `finally`
    block, preventing connection pool exhaustion under heavy load.
    """
    db: Session = SessionLocal()
    try:
        yield db
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        logger.error(f"Database session exception encountered: {exc}")
        db.rollback()
        raise
    finally:
        db.close()
