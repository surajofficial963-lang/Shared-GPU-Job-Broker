import json
from http.client import HTTPException
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .store import retention_cutoff


class ArchiveUnavailable(Exception):
    pass


class RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ClickHouseArchive:
    def __init__(self, url, user, password, database="broker", allow_http=False):
        parsed = urlsplit(url)
        allowed = {"https", "http"} if allow_http else {"https"}
        if (
            parsed.scheme not in allowed or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment
        ):
            raise ValueError("Invalid ClickHouse URL (HTTPS required by default)")
        self.url = url.rstrip("/") + "/"
        self.user = user
        self.password = password
        self.database = database
        self.opener = build_opener(RejectRedirects())

    def _query(self, sql, body=None, **params):
        query = urlencode({"database": self.database, "wait_end_of_query": 1, **params})
        if body is None:
            data = sql.encode()
        else:
            query += "&" + urlencode({"query": sql})
            data = body
        request = Request(
            self.url + "?" + query, data=data,
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
        )
        try:
            with self.opener.open(request, timeout=5) as response:
                return response.read().decode()
        except (OSError, HTTPException, UnicodeError) as exc:
            raise ArchiveUnavailable("ClickHouse archive is unavailable") from exc

    def save(self, table, rows):
        start_column = "started" if table == "jobs" else "launched"
        body = "\n".join(json.dumps({
            "kind": table, "id": row["id"], "team": row["team"],
            "started": row[start_column], "ended": row["ended"],
            "payload": json.dumps(dict(row), separators=(",", ":")),
        }) for row in rows)
        self._query(
            "INSERT INTO history SETTINGS async_insert = 0 FORMAT JSONEachRow", body.encode()
        )

    def get(self, table, record_id):
        data = self._query(
            "SELECT payload FROM history WHERE kind = {kind:String} AND id = {id:String} "
            "LIMIT 1 FORMAT JSONEachRow", param_kind=table, param_id=record_id,
        )
        rows = self._decode(data)
        return rows[0] if rows else None

    def window(self, table, begin, end, team=None):
        sql = (
            "SELECT DISTINCT payload FROM history WHERE kind = {kind:String} "
            "AND ended > {begin:Int64} AND started < {end:Int64}"
        )
        params = {"param_kind": table, "param_begin": begin, "param_end": end}
        if team is not None:
            sql += " AND team = {team:String}"
            params["param_team"] = team
        data = self._query(sql + " FORMAT JSONEachRow", **params)
        return self._decode(data)

    @staticmethod
    def _decode(data):
        try:
            return [json.loads(json.loads(line)["payload"]) for line in data.splitlines()]
        except (ValueError, KeyError, TypeError) as exc:
            raise ArchiveUnavailable("Invalid archive response") from exc


def archive_once(store, archive, limit=500):
    total = 0
    for table in ("jobs", "nodes"):
        with store.session() as db:
            now = db.execute("SELECT now FROM clock WHERE id = 1").fetchone()[0]
            cutoff = retention_cutoff(now)
            rows = db.execute(
                f"SELECT * FROM {table} WHERE ended <= ? ORDER BY ended LIMIT ?", (cutoff, limit)
            ).fetchall()
        if rows:
            archive.save(table, rows)
            # Rows are immutable once terminal. Delete only after ClickHouse acknowledges the batch.
            with store.session(write=True) as db:
                db.executemany(f"DELETE FROM {table} WHERE id = ?", [(row["id"],) for row in rows])
            total += len(rows)
    if total:
        with store.session(write=True) as db:
            db.execute("PRAGMA incremental_vacuum(128)")
    return total
