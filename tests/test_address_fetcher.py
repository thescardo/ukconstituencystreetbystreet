from datetime import datetime, timedelta
from typing import List, Optional

import pytest
from sqlalchemy.orm import Session

from ukconstituencystreetbystreet.address_fetcher import AddrFetcher
from ukconstituencystreetbystreet.db import db_repr_sqlite as db_repr

from conftest import TEST_CACHE_DB_FILE, get_test_engine


@pytest.fixture(scope="function")
def clear_api_req_table():
    def delete():
        with Session(get_test_engine()) as db_sess:
            db_sess.query(db_repr.ApiUseLog).delete()
            db_sess.commit()

    delete()
    yield
    delete()


@pytest.mark.parametrize(
    "minute_by_minute_use,api_use_last_db_read_older",
    [
        (None, False),
        (None, True),
        ([0, 286, 12, 100, 1000, 300], False),
        ([100, 286, 12, 100, 1000, 300], True),
        ([100, 286, 2000, 100, 1000, 300], False),
        ([100, 286, 2000, 100, 1000, 300], True),
    ],
)
def test_get_api_req_count_last_5_minutes(
    clear_api_req_table,
    minute_by_minute_use: Optional[List[int]],
    api_use_last_db_read_older: bool,
):
    fake_datetime_now = datetime(year=2000, month=11, day=1, hour=15, minute=17)
    fake_count_this_min = 123

    if minute_by_minute_use is not None:
        # Fill in previous 6 minutes
        count = -6
        with Session(db_repr.get_engine()) as db_sess:
            for x in minute_by_minute_use:
                assert count < 0
                db_sess.add(
                    db_repr.ApiUseLog(
                        minute=fake_datetime_now - timedelta(minutes=count),
                        num_requests=x,
                    )
                )
                count -= 1
            db_sess.commit()

    addr_fetcher = AddrFetcher()
    addr_fetcher._get_floored_minute_now = lambda: fake_datetime_now
    addr_fetcher._api_use_counter_this_min = fake_count_this_min

    if api_use_last_db_read_older:
        addr_fetcher._api_use_last_db_read = fake_datetime_now
    else:
        addr_fetcher._api_use_last_db_read = fake_datetime_now + timedelta(minutes=1)

    returned = addr_fetcher._get_api_req_count_last_5_minutes()

    if minute_by_minute_use is None or api_use_last_db_read_older:
        assert returned == fake_count_this_min
    else:
        assert returned == sum(minute_by_minute_use[-5:]) + fake_count_this_min
