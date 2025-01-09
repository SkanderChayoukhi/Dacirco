from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker


def db_engine_session(
    db_url: str = "sqlite:///./dacirco-controller.db",
) -> tuple[Engine, sessionmaker[Session]]:
    engine = create_engine(db_url, connect_args={"check_same_thread": False})

    # Enable foreign key constraint checks in SQLite
    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute("pragma foreign_keys=ON")

    event.listen(engine, "connect", _fk_pragma_on_connect)
    session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, session_maker
