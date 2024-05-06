def multiprocess_init(l, e):
    global db_write_lock, engine
    db_write_lock = l
    engine = e

    engine.dispose(close=False)
