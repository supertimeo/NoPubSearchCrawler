from sqlalchemy import String, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class URL(Base):
    __tablename__ = "urls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(2048), unique=True, index=True)


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )

    url: Mapped["URL"] = relationship(cascade="")

    title: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True
    )

    content: Mapped[str] = mapped_column(Text)

    links: Mapped[list["Link"]] = relationship(
        back_populates="page",
        cascade="all, delete-orphan"
    )


class Link(Base):
    __tablename__ = "links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    page_id: Mapped[int] = mapped_column(
        ForeignKey("pages.id")
    )

    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id")
    )

    page: Mapped[Page] = relationship(
        back_populates="links",
        cascade=""
    )

    url: Mapped[URL] = relationship()

    __table_args__ = (
        UniqueConstraint("page_id", "url_id"),
    )


class WaitingURL(Base):
    __tablename__ = "waiting_list"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )
    url: Mapped["URL"] = relationship(cascade="")
    priority: Mapped[int] = mapped_column()
    domain_crawled_at: Mapped[float] = mapped_column(index=True)

    __table_args__ = {"prefixes": ["UNLOGGED"]}


class CrawledURL(Base):
    __tablename__ = "crawled_urls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )
    url: Mapped["URL"] = relationship(cascade="")