"""测试辅助函数（异步）。"""


async def commit(client, changes):
    resp = await client.post("/commits", json={"changes": changes})
    assert resp.status_code == 200, resp.text
    return resp.json()["commit"]


async def query(client, device_ids, min_commit=0, max_wait_ms=0):
    params = [("device_id", d) for d in device_ids]
    params.append(("min_commit", min_commit))
    params.append(("max_wait_ms", max_wait_ms))
    return await client.get("/devices/params", params=params)
