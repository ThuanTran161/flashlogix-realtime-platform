from sqlalchemy import Column, Integer, String, Float
from app.core.database import Base


class Product(Base):
    """Product entity for flash sale inventory."""

    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False, default=0)
    version = Column(Integer, default=1, nullable=False)  # For Optimistic Locking support

    def __repr__(self) -> str:
        return f"<Product(id={self.id}, name='{self.name}', stock={self.stock})>"
